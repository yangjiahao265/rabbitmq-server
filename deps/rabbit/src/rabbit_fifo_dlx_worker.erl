%% This module consumes from a single quroum queue's discards queue (containing dead-letttered messages)
%% and forwards the DLX messages at least once to every target queue.
%%
%% Some parts of this module resemble the channel process in the sense that it needs to keep track what messages
%% are consumed but not acked yet and what messages are published but not confirmed yet.
%% Compared to the channel process, this module is protocol independent since it doesn't deal with AMQP clients.
%%
%% This module consumes directly from the rabbit_fifo_dlx_client bypassing the rabbit_queue_type interface,
%% but publishes via the rabbit_queue_type interface.
%% While consuming via rabbit_queue_type interface would have worked in practice (by using a special consumer argument,
%% e.g. {<<"x-internal-queue">>, longstr, <<"discards">>} ) using the rabbit_fifo_dlx_client directly provides
%% separation of concerns making things much easier to test, to debug, and to understand.

-module(rabbit_fifo_dlx_worker).

-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(gen_server2).

-export([start_link/2]).
%% gen_server2 callbacks
-export([init/1, terminate/2, handle_continue/2,
         handle_cast/2, handle_call/3, handle_info/2,
         code_change/3]).

%%TODO make configurable or leave at 0 which means 2000 as in
%% https://github.com/rabbitmq/rabbitmq-server/blob/1e7df8c436174735b1d167673afd3f1642da5cdc/deps/rabbit/src/rabbit_quorum_queue.erl#L726-L729
-define(CONSUMER_PREFETCH_COUNT, 10).
-define(HIBERNATE_AFTER, 180_000).
%% If no publisher confirm was received for at least SETTLE_TIMEOUT, message will be redelivered.
%% To prevent duplicates in the target queue and to ensure message will eventually be acked to the source queue,
%% set this value higher than the maximum time it takes for a queue to settle a message.
-define(SETTLE_TIMEOUT, 120_000).

-record(pending, {
          %% consumed_msg_id is not to be confused with consumer delivery tag.
          %% The latter represents a means for AMQP clients to (multi-)ack to a channel process.
          %% However, queues are not aware of delivery tags.
          %% This rabbit_fifo_dlx_worker does not have the concept of delivery tags because it settles (acks)
          %% message IDs directly back to the queue (and there is no AMQP consumer).
          consumed_msg_id :: non_neg_integer(),
          content :: rabbit_types:decoded_content(),
          %% target queues for which publisher confirm has not been received yet
          unsettled :: [rabbit_amqqueue:name()],
          %% target queues for which publisher confirm was received
          settled :: [rabbit_amqqueue:name()],
          %% number of times the message was (tried to be) published
          count :: non_neg_integer(),
          %% epoch time in milliseconds when the message was last (tried to be) published
          last_publish :: integer()
         }).

-record(state, {
          registered_name :: atom(),
          %% In this version of the module, we have one rabbit_fifo_dlx_worker per quorum queue
          %% (if x-dead-letter-strategy at-least-once is used).
          %% Hence, there is a single queue we consume from.
          consumer_queue_ref :: rabbit_amqqueue:name(),
          dlx_client_state :: rabbit_fifo_dlx_client:state(),
          queue_type_state :: rabbit_queue_type:state(),
          %% Consumed messages for which we have not received all publisher confirms yet.
          %% Therefore, they have not been ACKed yet to the consumer queue.
          %% This buffer contains at most CONSUMER_PREFETCH_COUNT pending messages at any given point in time.
          pendings = #{} :: #{OutSeq :: non_neg_integer() => #pending{}},
          %% next publisher confirm delivery tag sequence number
          next_out_seq = 1,
          %% Timer firing every SETTLE_TIMEOUT milliseconds
          %% redelivering messages for which not all publisher confirms were received.
          %% If there are no pending messages, this timer will eventually be cancelled to allow
          %% this worker to hibernate.
          timer :: reference()
         }).

-type state() :: #state{}.

start_link(QRef, RegName) ->
    gen_server:start_link({local, RegName},
                          ?MODULE, {QRef, RegName},
                          [{hibernate_after, ?HIBERNATE_AFTER}]).

-spec init({rabbit_amqqueue:name(), atom()}) -> {ok, state()}.
init(Arg) ->
    {ok, #state{}, {continue, Arg}}.

handle_continue({QRef, RegName}, State) ->
    {ok, Q} = rabbit_amqqueue:lookup(QRef),
    {ClusterName, _MaybeOldLeaderNode} = amqqueue:get_pid(Q),
    {ok, ConsumerState} = rabbit_fifo_dlx_client:checkout(RegName,
                                                          QRef,
                                                          {ClusterName, node()},
                                                          ?CONSUMER_PREFETCH_COUNT),
    {noreply, State#state{registered_name = RegName,
                          consumer_queue_ref = QRef,
                          dlx_client_state = ConsumerState,
                          queue_type_state = rabbit_queue_type:init()}}.

terminate(_Reason, _State) ->
    %% cancel subscription?
    ok.

handle_call(Request, From, State) ->
    rabbit_log:warning("~s received unhandled call from ~p: ~p", [?MODULE, From, Request]),
    {noreply, State}.

handle_cast({queue_event, QRef, {From, Evt}},
            #state{consumer_queue_ref = QRef,
                   dlx_client_state = DlxState0} = State0) ->
    %% received dead-letter messsage from source queue
    % rabbit_log:debug("~s received queue event: ~p", [rabbit_misc:rs(QRef), E]),
    {ok, DlxState, Actions} = rabbit_fifo_dlx_client:handle_ra_event(From, Evt, DlxState0),
    State1 = State0#state{dlx_client_state = DlxState},
    State = handle_queue_actions(Actions, State1),
    {noreply, State};
handle_cast({queue_event, QRef, Evt},
            #state{queue_type_state = QTypeState0} = State0) ->
    %% received e.g. confirm from target queue
    case rabbit_queue_type:handle_event(QRef, Evt, QTypeState0) of
        {ok, QTypeState1, Actions} ->
            State1 = State0#state{queue_type_state = QTypeState1},
            State = handle_queue_actions(Actions, State1),
            {noreply, State};
        %% TODO handle as done in
        %% https://github.com/rabbitmq/rabbitmq-server/blob/9cf18e83f279408e20430b55428a2b19156c90d7/deps/rabbit/src/rabbit_channel.erl#L771-L783
        eol ->
            {noreply, State0};
        {protocol_error, _Type, _Reason, _ReasonArgs} ->
            {noreply, State0}
    end;
handle_cast(settle_timeout, State0) ->
    State1 = State0#state{timer = undefined},
    State2 = redeliver_timed_out_messsages(State1),
    %% Routes could have been changed dynamically.
    %% If a publisher confirm timed out for a target queue to which we now don't route anymore, ack the message.
    State3 = maybe_ack(State2),
    State4 = maybe_set_timer(State3),
    {noreply, State4};
handle_cast(Request, State) ->
    rabbit_log:warning("~s received unhandled cast ~p", [?MODULE, Request]),
    {noreply, State}.

%%TODO handle monitor messages when target queue goes down (e.g. is deleted)
%% {'DOWN', #Ref<0.1329999082.3399994753.45999>,process,<0.2626.0>,normal}
%% and remove it from the queue_type_state
handle_info(Info, State) ->
    rabbit_log:warning("~s received unhandled info ~p", [?MODULE, Info]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% https://github.com/rabbitmq/rabbitmq-server/blob/9cf18e83f279408e20430b55428a2b19156c90d7/deps/rabbit/src/rabbit_channel.erl#L2855-L2888
handle_queue_actions(Actions, State0) ->
    lists:foldl(
      fun ({deliver, Msgs}, S0) ->
              S1 = handle_deliver(Msgs, S0),
              maybe_set_timer(S1);
          ({settled, QRef, MsgSeqs}, S0) ->
              S1 = handle_settled(QRef, MsgSeqs, S0),
              S2 = maybe_ack(S1),
              maybe_cancel_timer(S2);
          ({rejected, _QRef, _MsgSeqNos}, S0) ->
              rabbit_log:error("queue action rejected not yet implemented", []),
              S0
      end, State0, Actions).

handle_deliver(Msgs, #state{consumer_queue_ref = QRef} = State) when is_list(Msgs) ->
    {DLXRef, DLX, DLRKey} = lookup_dlx(QRef),
    lists:foldl(fun({_QRef, MsgId, #basic_message{content = Content}}, S) ->
                        deliver(Content, DLXRef, DLX, DLRKey, MsgId, 0, [], S)
                end, State, Msgs).

deliver(Content, DLXRef, DLX, DLRKey, ConsumedMsgId, Count, Settled,
        #state{next_out_seq = OutSeq,
               pendings = Pendings} = State0) ->
    {ok, BasicMsg} = rabbit_basic:message(DLXRef, DLRKey, Content),
    %% Field 'mandatory' is set to false because our module checks on its own whether the message is routable.
    Delivery = rabbit_basic:delivery(_Mandatory = false, _Confirm = true, BasicMsg, OutSeq),
    QNames = rabbit_exchange:route(DLX, Delivery),
    %% When this is a re-deliver, we won't send to queues for which we already received a publisher confirm.
    RouteToQNames = QNames -- Settled,
    Pend = #pending{
              consumed_msg_id = ConsumedMsgId,
              content = Content,
              unsettled = RouteToQNames,
              settled = Settled,
              count = Count + 1,
              last_publish = os:system_time(millisecond)
             },
    State1 = State0#state{next_out_seq = OutSeq + 1,
                          pendings = maps:put(OutSeq, Pend, Pendings)},
    case RouteToQNames of
        [] when Settled =:= []->
            rabbit_log:warning("Cannot deliver message with sequence number ~b "
                               "(for consumed message sequence number ~b) "
                               "because no queue is bound to dead-letter ~s with routing key '~s'.",
                               [OutSeq, ConsumedMsgId, rabbit_misc:rs(DLXRef), DLRKey]),
            State1;
        [] ->
            %% Original delivery timed out on publisher confirm.
            %% However, route changed dynamically so that we don't need to route to any queues anymore.
            %% This message will be acked later on.
            State1;
        _ ->
            %% This is the normal case. We (re)deliver message to target queues.
            deliver_to_queues(Delivery, RouteToQNames, State1)
    end.

deliver_to_queues(Delivery, RouteToQNames, #state{queue_type_state = QTypeState0} = State0) ->
    Qs = rabbit_amqqueue:lookup(RouteToQNames),
    {ok, QTypeState1, Actions} = rabbit_queue_type:deliver(Qs, Delivery, QTypeState0),
    State = State0#state{queue_type_state = QTypeState1},
    % rabbit_global_counters:messages_routed(amqp091, length(Qs)),
    handle_queue_actions(Actions, State).

%% Lookup policies and routes before delivering because they can change dynamically.
%% TODO rabbitmq_quorum_queue:policy_changed/1 should notify us when policy changes instead of doing a lookup
%% here every time
lookup_dlx({resource, Vhost, queue, _} = QRef) ->
    {ok, Q} = rabbit_amqqueue:lookup(QRef),
    DLRKey = rabbit_queue_type_util:args_policy_lookup(<<"dead-letter-routing-key">>, fun res_arg/2, Q),
    DLXName = rabbit_queue_type_util:args_policy_lookup(<<"dead-letter-exchange">>, fun res_arg/2, Q),
    DLXRef = rabbit_misc:r(Vhost, exchange, DLXName),
    DLX = rabbit_exchange:lookup_or_die(DLXRef),
    {DLXRef, DLX, DLRKey}.

handle_settled(QRef, MsgSeqs, #state{pendings = Pendings0} = State0) ->
    Pendings1 = lists:foldl(fun (MsgSeq, P0) ->
                                    handle_settled0(QRef, MsgSeq, P0)
                            end, Pendings0, MsgSeqs),
    State0#state{pendings = Pendings1}.

handle_settled0(QRef, MsgSeq, Pendings) ->
    case maps:find(MsgSeq, Pendings) of
        {ok, #pending{unsettled = Unset0, settled = Set0} = Pend0} ->
            Unset1 = lists:delete(QRef, Unset0),
            Set1 = [QRef | Set0],
            Pend1 = Pend0#pending{unsettled = Unset1, settled = Set1},
            maps:update(MsgSeq, Pend1, Pendings);
        error ->
            rabbit_log:warning("Ignoring publisher confirm for sequence number ~b "
                               "from target dead letter ~s after settle timeout of ~bms. "
                               "Troubleshoot why that queue confirms so slowly.",
                               [MsgSeq, rabbit_misc:rs(QRef), ?SETTLE_TIMEOUT])
    end.

maybe_ack(#state{pendings = Pendings0,
                 dlx_client_state = DlxState0} = State0) ->
    Settled = maps:filter(fun(_OutSeq, #pending{unsettled = [], settled = [_|_]}) ->
                                  %% Ack because there is at least one target queue and all
                                  %% target queues settled (i.e. combining publisher confirm
                                  %% and mandatory flag semantics).
                                  true;
                             (_, _) ->
                                  false
                          end, Pendings0),
    case maps:size(Settled) of
        0 ->
            %% nothing to ack
            State0;
        _ ->
            Ids = lists:map(fun(#pending{consumed_msg_id = Id}) -> Id end, maps:values(Settled)),
            case rabbit_fifo_dlx_client:settle(Ids, DlxState0) of
                {ok, DlxState} ->
                    SettledOutSeqs = maps:keys(Settled),
                    Pendings = maps:without(SettledOutSeqs, Pendings0),
                    State0#state{pendings = Pendings,
                                 dlx_client_state = DlxState};
                {error, _Reason} ->
                    %% Failed to ack. Ack will be retried in the next maybe_ack/1
                    State0
            end
    end.

redeliver_timed_out_messsages(#state{pendings = Pendings,
                                     consumer_queue_ref = SourceQRef} = State) ->
    Now = os:system_time(millisecond),
    {DLXRef, DLX, DLRKey} = lookup_dlx(SourceQRef),
    maps:fold(fun(OutSeq, #pending{consumed_msg_id = CMsgId,
                                   last_publish = LastPub,
                                   count = Count,
                                   content = Content,
                                   unsettled = Unsettled,
                                   settled = Settled}, S0) when LastPub + ?SETTLE_TIMEOUT =< Now ->
                      %% Publisher confirm timed out.
                      %%
                      %% Quorum queues maintain their own Raft sequene number mapping to the message sequence number (= Raft correlation ID).
                      %% So, they would just send us a 'settled' queue action containing the correct message sequence number.
                      %%
                      %% Classic queues however maintain their state by mapping the message sequence number to pending and confirmed queues.
                      %% While re-using the same message sequence number could work there as well, it just gets unnecssary complicated when
                      %% different target queues settle two separate deliveries referring to the same message sequence number (and same basic message).
                      %%
                      %% Therefore, to keep things simple, create a brand new delivery, store it in our state and forget about the old delivery and
                      %% sequence number.
                      %%
                      %% If a sequene number gets settled after SETTLE_TIMEOUT, we can't map it anymore to the #pending{}. Hence, we ignore it.
                      %%
                      %% This can lead to issues when SETTLE_TIMEOUT is too low and time to settle takes too long.
                      %% For example, if SETTLE_TIMEOUT is set to only 10 seconds, but settling a message takes always longer than 10 seconds
                      %% (e.g. due to extremly slow hypervisor disks that ran out of credit), we will re-deliver the same message all over again
                      %% leading to many duplicates in the target queue without ever acking the message back to the source discards queue.
                      %%
                      %% Therefore, set SETTLE_TIMEOUT reasonably high (e.g. 2 minutes).
                      rabbit_log:debug("Redelivering message with sequence number ~b (for consumed message sequence number ~b) "
                                       "that has been published ~b time(s) because of time out after ~bms waiting on publisher confirm. "
                                       "Received confirm from: [~s]. Did not receive confirm from: [~s].",
                                       [OutSeq, CMsgId, Count, ?SETTLE_TIMEOUT, strings(Settled), strings(Unsettled)]),
                      #state{pendings = Pends0} = S = deliver(Content, DLXRef, DLX, DLRKey, CMsgId, Count, Settled, S0),
                      Pends1 = maps:remove(OutSeq, Pends0),
                      S#state{pendings = Pends1};
                 (_OutSeq, _Pending, S) ->
                      %% Publisher confirm did not time out (yet).
                      S
              end, State, Pendings).

% name({resource, Vhost, queue, Queue}) ->
    % <<"internal-dead-letter-", Vhost/binary, "-", Queue/binary>>.

strings(QRefs) when is_list(QRefs) ->
    L0 = lists:map(fun rabbit_misc:rs/1, QRefs),
    L1 = lists:join(", ", L0),
    lists:flatten(L1).

res_arg(_PolVal, ArgVal) -> ArgVal.

maybe_set_timer(#state{timer = TRef} = State) when is_reference(TRef) ->
    State;
maybe_set_timer(#state{timer = undefined,
                       pendings = Pendings} = State) when map_size(Pendings) =:= 0 ->
    State;
maybe_set_timer(#state{timer = undefined} = State) ->
    TRef = erlang:send_after(?SETTLE_TIMEOUT, self(), {'$gen_cast', settle_timeout}),
    % rabbit_log:debug("set timer"),
    State#state{timer = TRef}.

maybe_cancel_timer(#state{timer = undefined} = State) ->
    State;
maybe_cancel_timer(#state{timer = TRef,
                          pendings = Pendings} = State) ->
    case maps:size(Pendings) of
        0 ->
            erlang:cancel_timer(TRef, [{async, true}, {info, false}]),
            % rabbit_log:debug("cancelled timer"),
            State#state{timer = undefined};
        _ ->
            State
    end.
