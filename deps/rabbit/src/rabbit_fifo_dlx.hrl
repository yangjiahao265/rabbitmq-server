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

-record(rabbit_fifo_dlx,{
          consumer = undefined :: #dlx_consumer{} | undefined,
          %% Queue of dead-lettered messages.
          discards = lqueue:new() :: lqueue:lqueue({reason(), indexed_msg()}),
          msg_bytes = 0 :: non_neg_integer(),
          msg_bytes_checkout = 0 :: non_neg_integer()
         }).
