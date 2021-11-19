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
-include_lib("rabbit_common/include/rabbit_framing.hrl").

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
          %% TODO Reason is already stored in first x-death header of #content.properties.#'P_basic'.headers
          %% So, we could remove this convenience field and lookup the 1st header when redelivering.
          reason :: rabbit_fifo_dlx:reason(),
          %% target queues for which publisher confirm has not been received yet
          unsettled = [] :: [rabbit_amqqueue:name()],
          %% target queues for which publisher confirm was received
          settled = [] :: [rabbit_amqqueue:name()],
          %% Number of times the message was published (i.e. rabbit_queue_type:deliver/3 invoked).
          %% Can be 0 if the message was never published (for example no route exists).
          publish_count = 0 :: non_neg_integer(),
          %% Epoch time in milliseconds when the message was last published (i.e. rabbit_queue_type:deliver/3 invoked).
          %% It can be 'undefined' if the message was never published (for example no route exists).
          last_published_at :: undefined | integer(),
          %% Epoch time in milliseconds when the message was consumed from the source quorum queue.
          %% This value never changes.
          %% It's mainly informational and meant for debugging to understand for how long the message
          %% is sitting around without having received all publisher confirms.
          consumed_at :: integer()
         }).

%%TODO export overview and don't dump state's messages binary data
-record(state, {
          registered_name :: atom(),
          %% In this version of the module, we have one rabbit_fifo_dlx_worker per source quorum queue
          %% (if x-dead-letter-strategy at-least-once is used).
          queue_ref :: rabbit_amqqueue:name(),
          %% configured (x-)dead-letter-exchange of source queue
          exchange_ref,
          %% configured (x-)dead-letter-routing-key of source queue
          routing_key,
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

% -type state() :: #state{}.

%%TODO add metrics like global counters for messages routed, delivered, etc.

start_link(QRef, RegName) ->
    gen_server:start_link({local, RegName},
                          ?MODULE, {QRef, RegName},
                          [{hibernate_after, ?HIBERNATE_AFTER}]).

-spec init({rabbit_amqqueue:name(), atom()}) -> {ok, undefined, {continue, {rabbit_amqqueue:name(), atom()}}}.
init(Arg) ->
    {ok, undefined, {continue, Arg}}.

handle_continue({QRef, RegName}, undefined) ->
    State = lookup_topology(#state{queue_ref = QRef}),
    {ok, Q} = rabbit_amqqueue:lookup(QRef),
    {ClusterName, _MaybeOldLeaderNode} = amqqueue:get_pid(Q),
    {ok, ConsumerState} = rabbit_fifo_dlx_client:checkout(RegName,
                                                          QRef,
                                                          {ClusterName, node()},
                                                          ?CONSUMER_PREFETCH_COUNT),
    {noreply, State#state{registered_name = RegName,
                          dlx_client_state = ConsumerState,
                          queue_type_state = rabbit_queue_type:init()}}.

terminate(_Reason, _State) ->
    %% cancel subscription?
    ok.

handle_call(Request, From, State) ->
    rabbit_log:warning("~s received unhandled call from ~p: ~p", [?MODULE, From, Request]),
    {noreply, State}.

handle_cast({queue_event, QRef, {_From, {machine, lookup_topology}}},
            #state{queue_ref = QRef} = State) ->
    {noreply, lookup_topology(State)};
handle_cast({queue_event, QRef, {From, Evt}},
            #state{queue_ref = QRef,
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

lookup_topology(#state{queue_ref = {resource, Vhost, queue, _} = QRef} = State) ->
    {ok, Q} = rabbit_amqqueue:lookup(QRef),
    DLRKey = rabbit_queue_type_util:args_policy_lookup(<<"dead-letter-routing-key">>, fun(Pol, _QArg) -> Pol end, Q),
    DLX = rabbit_queue_type_util:args_policy_lookup(<<"dead-letter-exchange">>, fun(Pol, _QArg) -> Pol end, Q),
    DLXRef = rabbit_misc:r(Vhost, exchange, DLX),
    State#state{exchange_ref = DLXRef,
                routing_key = DLRKey}.

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

handle_deliver(Msgs, #state{queue_ref = QRef} = State) when is_list(Msgs) ->
    DLX = lookup_dlx(State),
    lists:foldl(fun({_QRef, MsgId, Msg, Reason}, S) ->
                        forward(Msg, MsgId, QRef, DLX, Reason, S)
                end, State, Msgs).

lookup_dlx(#state{exchange_ref = DLXRef,
                  queue_ref = QRef}) ->
    case rabbit_exchange:lookup(DLXRef) of
        {error, not_found} ->
            rabbit_log:warning("Cannot forward any dead-letter messages from source quorum ~s because its configured "
                               "dead-letter-exchange ~s does not exist. "
                               "Either create the configured dead-letter-exchange or re-configure "
                               "the dead-letter-exchange policy for the source quorum queue to prevent "
                               "dead-lettered messages from piling up in the source quorum queue.",
                               [rabbit_misc:rs(QRef), rabbit_misc:rs(DLXRef)]),
            not_found;
        {ok, X} ->
            X
    end.

forward(ConsumedMsg, ConsumedMsgId, ConsumedQRef, DLX, Reason,
        #state{next_out_seq = OutSeq,
               pendings = Pendings,
               exchange_ref = DLXRef,
               routing_key = RKey} = State0) ->
    #basic_message{content = Content, routing_keys = RKeys} = Msg =
    rabbit_dead_letter:make_msg(ConsumedMsg, Reason, DLXRef, RKey, ConsumedQRef),
    %% Field 'mandatory' is set to false because our module checks on its own whether the message is routable.
    Delivery = rabbit_basic:delivery(_Mandatory = false, _Confirm = true, Msg, OutSeq),
    TargetQs = case DLX of
                   not_found ->
                       [];
                   _ ->
                       RouteToQs = rabbit_exchange:route(DLX, Delivery),
                       case rabbit_dead_letter:detect_cycles(Reason, Msg, RouteToQs) of
                           {[], []} ->
                               rabbit_log:warning("Cannot deliver message with sequence number ~b "
                                                  "(for consumed message sequence number ~b) "
                                                  "because no queue is bound to dead-letter ~s with routing key '~s'.",
                                                  [OutSeq, ConsumedMsgId, rabbit_misc:rs(DLXRef), RKey]),
                               [];
                           {Qs, []} ->
                               %% the "normal" case, i.e. no dead-letter-topology misconfiguration
                               Qs;
                           {[], Cycles} ->
                               %%TODO introduce structured logging in rabbit_log by using type logger:report
                               rabbit_log:warning("Cannot route to any queues. Detected dead-letter queue cycles. "
                                                  "Fix the dead-letter routing topology to prevent dead-letter messages from "
                                                  "piling up in source quorum queue. "
                                                  "outgoing_sequene_number=~b "
                                                  "consumed_message_sequence_number=~b "
                                                  "consumed_queue=~s "
                                                  "dead_letter_exchange=~s "
                                                  "effective_dead_letter_routing_keys=~p "
                                                  "routed_to_queues=~s "
                                                  "dead_letter_queue_cycles=~p",
                                                  [OutSeq, ConsumedMsgId, rabbit_misc:rs(ConsumedQRef),
                                                   rabbit_misc:rs(DLXRef), RKeys, strings(RouteToQs), Cycles]),
                               [];
                           {Qs, Cycles} ->
                               rabbit_log:warning("Detected dead-letter queue cycles. "
                                                  "Fix the dead-letter routing topology. "
                                                  "outgoing_sequene_number=~b "
                                                  "consumed_message_sequence_number=~b "
                                                  "consumed_queue=~s "
                                                  "dead_letter_exchange=~s "
                                                  "effective_dead_letter_routing_keys=~p "
                                                  "routed_to_queues_desired=~s "
                                                  "routed_to_queues_effective=~s "
                                                  "dead_letter_queue_cycles=~p",
                                                  [OutSeq, ConsumedMsgId, rabbit_misc:rs(ConsumedQRef),
                                                   rabbit_misc:rs(DLXRef), RKeys, strings(RouteToQs), strings(Qs), Cycles]),
                               %% Ignore the target queues resulting in cycles.
                               %% We decide it's good enough to deliver to only routable target queues.
                               Qs
                       end
               end,
    Now = os:system_time(millisecond),
    State1 = State0#state{next_out_seq = OutSeq + 1},
    Pend0 = #pending{
               consumed_msg_id = ConsumedMsgId,
               consumed_at = Now,
               content = Content,
               reason = Reason
              },
    case TargetQs of
        [] ->
            %% We can't deliver this message since there is no target queue we can route to.
            %% Under no circumstances should we drop a message with dead-letter-strategy at-least-once.
            %% We buffer this message and retry to send every SETTLE_TIMEOUT milliseonds
            %% (until the user has fixed the dead-letter routing topology).
            State1#state{pendings = maps:put(OutSeq, Pend0, Pendings)};
        _ ->
            Pend = Pend0#pending{publish_count = 1,
                                 last_published_at = Now,
                                 unsettled = TargetQs},
            State = State1#state{pendings = maps:put(OutSeq, Pend, Pendings)},
            deliver_to_queues(Delivery, TargetQs, State)
    end.

deliver_to_queues(Delivery, RouteToQNames, #state{queue_type_state = QTypeState0} = State0) ->
    Qs = rabbit_amqqueue:lookup(RouteToQNames),
    {ok, QTypeState1, Actions} = rabbit_queue_type:deliver(Qs, Delivery, QTypeState0),
    State = State0#state{queue_type_state = QTypeState1},
    handle_queue_actions(Actions, State).

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

redeliver_timed_out_messsages(#state{pendings = Pendings} = State) ->
    case lookup_dlx(State) of
        not_found ->
            %% Configured dead-letter-exchange does (still) not exist.
            %% Warning got already logged.
            %% Keep the same Pendings in our state until user creates or re-configures the dead-letter-exchange.
            State;
        DLX ->
            Now = os:system_time(millisecond),
            maps:fold(fun(OutSeq, #pending{last_published_at = LastPub} = Pend, S0) when LastPub + ?SETTLE_TIMEOUT =< Now ->
                              %% Publisher confirm timed out.
                              redeliver(Pend, DLX, OutSeq, S0);
                         (OutSeq, #pending{last_published_at = undefined,
                                           consumed_at = ConsumedAt} = Pend, S0) when ConsumedAt + ?SETTLE_TIMEOUT =< Now ->
                              %% Message was never published due to dead-letter routing topology misconfiguration.
                              redeliver(Pend, DLX, OutSeq, S0);
                         (_OutSeq, _Pending, S) ->
                              %% Publisher confirm did not time out (yet).
                              S
                      end, State, Pendings)
    end.

redeliver(#pending{content = Content} = Pend, DLX, OldOutSeq,
          #state{routing_key = undefined} = State) ->
    %% No dead-letter-routing-key defined for source quorum queue.
    %% Therefore use all of messages's original routing keys (which can include CC and BCC recipients).
    %% This complies with the behaviour of the rabbit_dead_letter module.
    %% We stored these original routing keys in the 1st (i.e. most recent) x-death entry.
    #content{properties = #'P_basic'{headers = Headers}} =
    rabbit_binary_parser:ensure_content_decoded(Content),
    {array, [{table, MostRecentDeath}|_]} = rabbit_misc:table_lookup(Headers, <<"x-death">>),
    {<<"routing-keys">>, array, Routes0} = lists:keyfind(<<"routing-keys">>, 1, MostRecentDeath),
    Routes = [Route || {longstr, Route} <- Routes0],
    redeliver0(Pend, DLX, Routes, OldOutSeq, State);
redeliver(Pend, DLX, OldOutSeq, #state{routing_key = DLRKey} = State) ->
    redeliver0(Pend, DLX, [DLRKey], OldOutSeq, State).

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
redeliver0(#pending{consumed_msg_id = ConsumedMsgId,
                    content = Content,
                    unsettled = Unsettled,
                    settled = Settled,
                    publish_count = PublishCount,
                    reason = Reason} = Pend0,
           DLX, DLRKeys, OldOutSeq,
           #state{next_out_seq = OutSeq,
                  queue_ref = QRef,
                  pendings = Pendings0,
                  exchange_ref = DLXRef} = State0) when is_list(DLRKeys) ->
    BasicMsg = #basic_message{exchange_name = DLXRef,
                              routing_keys  = DLRKeys,
                              %% BCC Header was already stripped previously
                              content       = Content,
                              id            = rabbit_guid:gen(),
                              is_persistent = rabbit_basic:is_message_persistent(Content)
                             },
    %% Field 'mandatory' is set to false because our module checks on its own whether the message is routable.
    Delivery = rabbit_basic:delivery(_Mandatory = false, _Confirm = true, BasicMsg, OutSeq),
    RouteToQs0 = rabbit_exchange:route(DLX, Delivery),
    %% Do not re-deliver to queues for which we already received a publisher confirm.
    RouteToQs1 = RouteToQs0 -- Settled,
    {RouteToQs, Cycles} = rabbit_dead_letter:detect_cycles(Reason, BasicMsg, RouteToQs1),
    Prefix = io_lib:format("Message timed out waiting on publisher confirm(s). "
                           "Received confirm from: [~s]. "
                           "Did not receive confirm from: [~s]. "
                           "timeout=~bms "
                           "message_sequence_number=~b "
                           "consumed_message_sequence_number=~b "
                           "publish_count=~b.",
                           [strings(Settled), strings(Unsettled), ?SETTLE_TIMEOUT,
                            OldOutSeq, ConsumedMsgId, PublishCount]),
    case {RouteToQs, Cycles, Settled} of
        {[], [], []} ->
            rabbit_log:warning("~s Failed to re-deliver this message because no queue is bound "
                               "to dead-letter ~s with routing keys ~p.",
                               [Prefix, rabbit_misc:rs(DLXRef), DLRKeys]),
            State0;
        {[], [], [_|_]} ->
            rabbit_log:debug("~s Routes changed dynamically so that this message does not need to be routed "
                             "to any queue anymore. This message will be acknowledged to the source ~s.",
                             [Prefix, rabbit_misc:rs(QRef)]),
            State0;
        {[], [_|_], []} ->
            rabbit_log:warning("~s Failed to re-deliver this message because dead-letter queue cycles "
                               "got detected: ~p",
                               [Prefix, Cycles]),
            State0;
        {[], [_|_], [_|_]} ->
            rabbit_log:warning("~s Dead-letter queue cycles detected: ~p. "
                               "This message will nevertheless be acknowledged to the source ~s "
                               "because it received at least one publisher confirm.",
                               [Prefix, Cycles, rabbit_misc:rs(QRef)]),
            State0;
        _ ->
            case Cycles of
                [] ->
                    rabbit_log:debug("~s Re-delivering this message to ~s",
                                     [Prefix, strings(RouteToQs)]);
                [_|_] ->
                    rabbit_log:warning("~s Dead-letter queue cycles detected: ~p. "
                                       "Re-delivering this message only to ~s",
                                       [Prefix, Cycles, strings(RouteToQs)])
            end,
            Pend = Pend0#pending{publish_count = PublishCount + 1,
                                 last_published_at = os:system_time(millisecond)},
            Pendings1 = maps:remove(OldOutSeq, Pendings0),
            Pendings = maps:put(OutSeq, Pend, Pendings1),
            State = State0#state{next_out_seq = OutSeq + 1,
                                 pendings = Pendings},
            deliver_to_queues(Delivery, RouteToQs, State)
    end.

strings(QRefs) when is_list(QRefs) ->
    L0 = lists:map(fun rabbit_misc:rs/1, QRefs),
    L1 = lists:join(", ", L0),
    lists:flatten(L1).

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
