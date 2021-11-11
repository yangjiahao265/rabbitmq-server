-module(rabbit_fifo_dlx_client).

-export([checkout/4, settle/2, handle_ra_event/3]).

-record(state,{
          queue_resource :: rabbit_tyes:r(queue),
          leader :: ra:server_id(),
          last_msg_id :: non_neg_integer | -1
         }).
-opaque state() :: #state{}.
-export_type([state/0]).

checkout(RegName, QResource, Leader, NumUnsettled) ->
    Cmd = rabbit_fifo_dlx:make_checkout(RegName, NumUnsettled),
    %% TODO
    %% A message being discarded and thereafter acked has to be persisted in the Raft log.
    %% However, DLX worker subscription doesn't necessarily have to be persisted in the Raft log, does it?
    %% For normal consumers it makes sense because the new leader needs resume delivering messages
    %% where the old leader left off. So it needs to know the various consumer sates.
    %% But the new leader will simply re-publish all unacked discarded messages to the new DLX worker, no need
    %% to keep the old DLX worker state around?
    State = #state{queue_resource = QResource,
                   leader = Leader,
                   last_msg_id = -1},
    process_command(Cmd, State, 5).

settle(MsgIds, State) when is_list(MsgIds) ->
    Cmd = rabbit_fifo_dlx:make_settle(MsgIds),
    process_command(Cmd, State, 2).

process_command(_Cmd, _State, 0) ->
    {error, ra_command_failed};
process_command(Cmd, #state{leader = Leader} = State, Tries) ->
    case ra:process_command(Leader, Cmd, 60_000) of
        {ok, ok, Leader} ->
            {ok, State#state{leader = Leader}};
        {ok, ok, L} ->
            rabbit_log:warning("Failed to process command ~p on quorum queue leader ~p because actual leader is ~p.",
                               [Cmd, Leader, L]),
            {error, ra_command_failed};
        Err ->
            rabbit_log:warning("Failed to process command ~p on quorum queue leader ~p: ~p~n"
                               "Trying ~b more time(s)...",
                               [Cmd, Leader, Err, Tries]),
            process_command(Cmd, State, Tries - 1)
    end.

handle_ra_event(Leader, {machine, {delivery, _} = Del}, #state{leader = Leader} = State) ->
    handle_delivery(Del, State);
handle_ra_event(_From, Evt, State) ->
    rabbit_log:warning("~s received unknown ra event: ~p", [?MODULE, Evt]),
    {ok, State, []}.

handle_delivery({delivery, [{FstId, _} | _] = IdMsgs},
                #state{queue_resource = QRes,
                       last_msg_id = Prev} = State0) ->
    %% format as a deliver action
    {LastId, _} = lists:last(IdMsgs),
    Del = {deliver, transform_msgs(QRes, IdMsgs)},
    case Prev of
        Prev when FstId =:= Prev+1 ->
            %% expected message ID(s) got delivered
            State = State0#state{last_msg_id = LastId},
            {ok, State, [Del]};
        Prev when FstId > Prev+1 ->
            %% messages ID(s) are missing, therefore fetch all checked-out discarded messages
            %% TODO implement as done in
            %% https://github.com/rabbitmq/rabbitmq-server/blob/b4eb5e2cfd7f85a1681617dc489dd347fa9aac72/deps/rabbit/src/rabbit_fifo_client.erl#L732-L744
            exit(not_implemented);
        Prev when FstId =< Prev ->
            rabbit_log:debug("dropping messages with duplicate IDs (~b to ~b) consumed from ~s",
                             [FstId, Prev, rabbit_misc:rs(QRes)]),
            case lists:dropwhile(fun({Id, _}) -> Id =< Prev end, IdMsgs) of
                [] ->
                    {ok, State0, []};
                IdMsgs2 ->
                    handle_delivery({delivery, IdMsgs2}, State0)
            end;
        _ when FstId =:= 0 ->
            % the very first delivery
            % TODO We init last_msg_id with -1. So, why would we ever run into this branch?
            rabbit_log:debug("very first delivery consumed from ~s", [rabbit_misc:rs(QRes)]),
            State = State0#state{last_msg_id = 0},
            {ok, State, [Del]}
    end.

transform_msgs(QRes, Msgs) ->
    lists:map(
      fun({MsgId, {Reason, _MsgHeader, Msg}}) ->
              {QRes, MsgId, Msg, Reason}
      end, Msgs).
