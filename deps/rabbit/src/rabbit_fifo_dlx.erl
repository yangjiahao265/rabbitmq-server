-module(rabbit_fifo_dlx).

-include("rabbit_fifo.hrl").

% client API, e.g. for rabbit_fifo_dlx_client
-export([make_checkout/2,
         make_settle/1]).

% called by rabbit_fifo delegating DLX handling to this module
-export([init/0, apply/2, discard/3, overview/1,
         checkout/1, state_enter/4,
         start_worker/2, terminate_worker/1, cleanup/1,
         consumer_pid/1]).

%% This module handles the dead letter (DLX) part of the rabbit_fifo state machine.
%% This is a separate module to better unit test and provide separation of concerns.
%% This module maintains its own state:
%% a queue of DLX messages, a single node local DLX consumer, and some stats.
%% The state of this module is included into rabbit_fifo state because there can only by one Ra state machine.
%% The rabbit_fifo module forwards all DLX commands to this module where we then update the DLX specific state only:
%% e.g. DLX consumer subscribed, adding / removing discarded messages, stats
%%
%% It also runs its own checkout logic sending DLX messages to the DLX consumer.
%%
%% TODO: Does it hook into the tick as well?
%%
%% At-least-once dead-lettering does not support reason 'maxlen'.
-type reason() :: 'expired' | 'rejected' | delivery_limit.

%%TODO Add logic to rabbit_fifo_dlx to dehydrate the state.
% See snapshot scenarios in rabbit_fifo_prop_SUITE. Add dlx dehydrate tests.
-record(dlx_consumer,{
          %% We don't require a consumer tag because a consumer tag is a means to distinguish
          %% multiple consumers in the same channel. The rabbit_fifo_dlx_worker channel like process however
          %% creates only a single consumer to this quorum queue's discards queue.
          registered_name :: atom(),
          prefetch :: non_neg_integer(),
          %%TODO use ?TUPLE for memory optimisation?
          checked_out = #{} :: #{msg_id() => {reason(), indexed_msg()}},
          next_msg_id = 0 :: msg_id() % part of snapshot data
          % total number of checked out messages - ever
          % incremented for each delivery
          % delivery_count = 0 :: non_neg_integer(),
          % status = up :: up | suspected_down | cancelled
         }).

-record(state,{
          consumer = undefined :: #dlx_consumer{} | undefined,
          %% Queue of dead-lettered messages.
          discards = lqueue:new() :: lqueue:lqueue({reason(), indexed_msg()}),
          msg_bytes = 0 :: non_neg_integer(),
          msg_bytes_checkout = 0 :: non_neg_integer()
         }).
-opaque state() :: #state{}.

-record(checkout,{
          consumer :: atom(),
          prefetch :: non_neg_integer()
         }).
-record(settle, {msg_ids :: [msg_id()]}).
-opaque protocol() :: {dlx, #checkout{} | #settle{}}.

-export_type([state/0, protocol/0, reason/0]).

init() ->
    #state{}.

make_checkout(RegName, NumUnsettled) ->
    {dlx, #checkout{consumer = RegName,
                    prefetch = NumUnsettled
                   }}.

make_settle(MessageIds) when is_list(MessageIds) ->
    {dlx, #settle{msg_ids = MessageIds}}.

overview(#state{consumer = undefined,
                msg_bytes = MsgBytes,
                msg_bytes_checkout = 0,
                discards = Discards}) ->
    overview0(Discards, #{}, MsgBytes, 0);
overview(#state{consumer = #dlx_consumer{checked_out = Checked},
                msg_bytes = MsgBytes,
                msg_bytes_checkout = MsgBytesCheckout,
                discards = Discards}) ->
    overview0(Discards, Checked, MsgBytes, MsgBytesCheckout).

overview0(Discards, Checked, MsgBytes, MsgBytesCheckout) ->
    #{num_discarded => lqueue:len(Discards),
      num_discard_checked_out => map_size(Checked),
      discard_message_bytes => MsgBytes,
      discard_checkout_message_bytes => MsgBytesCheckout}.

apply(#checkout{consumer = RegName,
                prefetch = Prefetch},
      #state{consumer = undefined} = State0) ->
    State = State0#state{consumer = #dlx_consumer{registered_name = RegName,
                                                  prefetch = Prefetch}},
    {State, ok};
apply(#checkout{consumer = RegName,
                prefetch = Prefetch},
      #state{consumer = #dlx_consumer{checked_out = CheckedOutOldConsumer},
             discards = Discards0,
             msg_bytes = Bytes,
             msg_bytes_checkout = BytesCheckout} = State0) ->
    %% Since we allow only a single consumer, the new consumer replaces the old consumer.
    %% All checked out messages to the old consumer need to be returned to the discards queue
    %% such that these messages can be (eventually) re-delivered to the new consumer.
    %% When inserting back into the discards queue, we respect the original order in which messages
    %% were discarded.
    Checked0 = maps:to_list(CheckedOutOldConsumer),
    Checked1 = lists:keysort(1, Checked0),
    {Discards, BytesMoved} = lists:foldr(fun({_Id, {_Reason, IdxMsg} = Msg}, {D, B}) ->
                                                 {lqueue:in_r(Msg, D), B + size_in_bytes(IdxMsg)}
                                         end, {Discards0, 0}, Checked1),
    State = State0#state{consumer = #dlx_consumer{registered_name = RegName,
                                                  prefetch = Prefetch},
                         discards = Discards,
                         msg_bytes = Bytes + BytesMoved,
                         msg_bytes_checkout = BytesCheckout - BytesMoved},
    {State, ok};
apply(#settle{msg_ids = MsgIds},
      #state{consumer = #dlx_consumer{checked_out = Checked} = C,
             msg_bytes_checkout = BytesCheckout} = State0) ->
    Acked = maps:with(MsgIds, Checked),
    AckedBytes = maps:fold(fun(_MsgId, {_Reason, Msg}, Bytes) ->
                                   Bytes + size_in_bytes(Msg)
                           end, 0, Acked),
    Unacked = maps:without(MsgIds, Checked),
    State = State0#state{consumer = C#dlx_consumer{checked_out = Unacked},
                         msg_bytes_checkout = BytesCheckout - AckedBytes},
    {State, Acked}.

discard(Msg, Reason, #state{discards = Discards0,
                            msg_bytes = MsgBytes0} = State) ->
    Discards = lqueue:in({Reason, Msg}, Discards0),
    MsgBytes = MsgBytes0 + size_in_bytes(Msg),
    State#state{discards = Discards,
                msg_bytes = MsgBytes}.

checkout(#state{consumer = undefined,
                discards = Discards} = State) ->
    case lqueue:is_empty(Discards) of
        true ->
            ok;
        false ->
            rabbit_log:warning("there are dead-letter messages but no dead-letter consumer")
    end,
    {State, []};
checkout(State) ->
    checkout0(checkout_one(State), {[],[]}).

checkout0({success, MsgId, {Reason, ?INDEX_MSG(RaftIdx, ?DISK_MSG(Header))}, State}, {InMemMsgs, LogMsgs}) when is_integer(RaftIdx) ->
    DelMsg = {RaftIdx, {Reason, MsgId, Header}},
    SendAcc = {InMemMsgs, [DelMsg|LogMsgs]},
    checkout0(checkout_one(State ), SendAcc);
checkout0({success, MsgId, {Reason, ?INDEX_MSG(Idx, ?MSG(Header, Msg))}, State}, {InMemMsgs, LogMsgs}) when is_integer(Idx) ->
    DelMsg = {MsgId, {Reason, Header, Msg}},
    SendAcc = {[DelMsg|InMemMsgs], LogMsgs},
    checkout0(checkout_one(State), SendAcc);
%TODO Is that a no-op for prefix messages after recovery?
% checkout0({success, _MsgId, ?TUPLE(_, _), State}, SendAcc) ->
% checkout0(checkout_one(State), SendAcc);
checkout0(#state{consumer = #dlx_consumer{registered_name = RegName}} = State, SendAcc) ->
    Effects = delivery_effects(whereis(RegName), SendAcc),
    {State, Effects}.

checkout_one(#state{consumer = #dlx_consumer{checked_out = Checked,
                                             prefetch = Prefetch}} = State) when map_size(Checked) >= Prefetch ->
    State;
checkout_one(#state{consumer = #dlx_consumer{checked_out = Checked0,
                                             next_msg_id = Next} = Con0} = State0) ->
    case take_next_msg(State0) of
        {{_, Msg} = ReasonMsg, State1} ->
            Checked = maps:put(Next, ReasonMsg, Checked0),
            State2 = State1#state{consumer = Con0#dlx_consumer{checked_out = Checked,
                                                               next_msg_id = Next + 1}},
            Bytes = size_in_bytes(Msg),
            State = add_bytes_checkout(Bytes, State2),
            {success, Next, ReasonMsg, State};
        empty ->
            State0
    end.

take_next_msg(#state{discards = Discards0} = State) ->
    case lqueue:out(Discards0) of
        {empty, _} ->
            empty;
        {{value, ReasonIndexMsg}, Discards} ->
            {ReasonIndexMsg, State#state{discards = Discards}}
    end.

add_bytes_checkout(Size, #state{msg_bytes = Bytes,
                                msg_bytes_checkout = BytesCheckout} = State) ->
    State#state{msg_bytes = Bytes - Size,
                msg_bytes_checkout = BytesCheckout + Size}.

size_in_bytes(Msg) ->
    Header = rabbit_fifo:get_msg_header(Msg),
    rabbit_fifo:get_header(size, Header).

%% returns at most one delivery effect because there is only one consumer
delivery_effects(_CPid, {[], []}) ->
    [];
delivery_effects(CPid, {InMemMsgs, []}) ->
    [{send_msg, CPid, {delivery, lists:reverse(InMemMsgs)}, [ra_event]}];
delivery_effects(CPid, {InMemMsgs, IdxMsgs0}) ->
    IdxMsgs = lists:reverse(IdxMsgs0),
    {RaftIdxs, Data} = lists:unzip(IdxMsgs),
    [{log, RaftIdxs,
      fun(Log) ->
              Msgs0 = lists:zipwith(fun ({enqueue, _, _, Msg}, {Reason, MsgId, Header}) ->
                                            {MsgId, {Reason, Header, Msg}}
                                    end, Log, Data),
              Msgs = case InMemMsgs of
                         [] ->
                             Msgs0;
                         _ ->
                             lists:sort(InMemMsgs ++ Msgs0)
                     end,
              [{send_msg, CPid, {delivery, Msgs}, [ra_event]}]
      end,
      {local, node(CPid)}}].

state_enter(leader, QRef, QName, _State) ->
    start_worker(QRef, QName);
state_enter(_, _, _, State) ->
    terminate_worker(State).

start_worker(QRef, QName) ->
    RegName = registered_name(QName),
    %% We must ensure that starting the rabbit_fifo_dlx_worker succeeds.
    %% Therefore, we don't use an effect.
    %% Also therefore, if starting the rabbit_fifo_dlx_worker fails, let the whole Ra server process crash
    %% in which case another Ra node will become leader.
    %% supervisor:start_child/2 blocks until rabbit_fifo_dlx_worker:init/1 returns (TODO check if this is correct).
    %% That's okay since rabbit_fifo_dlx_worker:init/1 returns immediately by delegating
    %% initial setup to handle_continue/2.
    case whereis(RegName) of
        undefined ->
            {ok, Pid} = supervisor:start_child(rabbit_fifo_dlx_sup, [QRef, RegName]),
            rabbit_log:debug("started rabbit_fifo_dlx_worker (~s ~p)", [RegName, Pid]);
        Pid ->
            rabbit_log:debug("rabbit_fifo_dlx_worker (~s ~p) already started", [RegName, Pid])
    end.

terminate_worker(#state{consumer = #dlx_consumer{registered_name = RegName}}) ->
    case whereis(RegName) of
        undefined ->
            ok;
        Pid ->
            %% Note that we can't return a mod_call effect here because mod_call is executed on the leader only.
            ok = supervisor:terminate_child(rabbit_fifo_dlx_sup, Pid),
            rabbit_log:debug("terminated rabbit_fifo_dlx_worker (~s ~p)", [RegName, Pid])
    end;
terminate_worker(_) ->
    ok.

%% TODO consider not registering the worker name at all
%% because if there is a new worker process, it will always subscribe and tell us its new pid
registered_name(QName) when is_atom(QName) ->
    list_to_atom(atom_to_list(QName) ++ "_dlx").

consumer_pid(#state{consumer = #dlx_consumer{registered_name = Name}}) ->
    whereis(Name);
consumer_pid(_) ->
    undefined.

%% called when switching from at-least-once to at-most-once
cleanup(#state{consumer = Consumer,
               discards = Discards} = State) ->
    terminate_worker(State),
    %% Return messages in the order they got discarded originally
    %% for the final at-most-once dead-lettering.
    CheckedReasonMsgs = case Consumer of
                            #dlx_consumer{checked_out = Checked} when is_map(Checked) ->
                                L0 = maps:to_list(Checked),
                                L1 = lists:keysort(1, L0),
                                {_, L2} = lists:unzip(L1),
                                L2;
                            _ ->
                                []
                        end,
    DiscardReasonMsgs = lqueue:to_list(Discards),
    CheckedReasonMsgs ++ DiscardReasonMsgs.
