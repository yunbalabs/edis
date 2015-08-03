%%%-------------------------------------------------------------------
%%% @author zy
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 24. 七月 2015 4:57 PM
%%%-------------------------------------------------------------------
-module(edis_dcp_monitor).
-author("zy").

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {seqno, producer_addr}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
  {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init([]) ->
  StartSeqNo = case read_log_position(1) of
                 {ok, Position} ->
                   Position;
                 _ ->
                   1
               end,
  ProducerAddr = edis_config:get(dcp_producer_addr, ["127.0.0.1", 12121]),
  self() ! synchronize,
  {ok, #state{seqno = StartSeqNo, producer_addr = ProducerAddr}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
  {reply, Reply :: term(), NewState :: #state{}} |
  {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_call(_Request, _From, State) ->
  {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_cast({snapshot_marker, SnapshotStart}, State) ->
  store_log_position(1, SnapshotStart),
  {noreply, State};
handle_cast({stream_error, SeqNo}, State) ->
  erlang:send_after(10000, self(), synchronize),
  {noreply, State#state{seqno = SeqNo + 1}};
handle_cast({stream_end, SeqNo}, State) ->
  erlang:send_after(10000, self(), synchronize),
  {noreply, State#state{seqno = SeqNo + 1}};
handle_cast(_Request, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_info(synchronize, State = #state{seqno = SeqNo, producer_addr = ProducerAddr}) ->
  start_synchronize(ProducerAddr, SeqNo),
  {noreply, State};
handle_info(_Info, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(_Reason, _State) ->
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
  {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
start_synchronize(ProducerAddr, StartSeqNo) ->
  case edis_dcp_handler:open_stream(ProducerAddr, [1, StartSeqNo, 0]) of
    {ok, _} ->
      ok;
    _ ->
      erlang:send_after(10000, self(), synchronize)
  end.

read_log_position(VBucketUUID) ->
  case ets:file2tab("log_position.dat") of
    {ok, Tab} ->
      Ret = case ets:lookup(Tab, VBucketUUID) of
              [{_, Pos}] ->
                {ok, Pos};
              [] ->
                {error, not_found}
            end,
      ets:delete(Tab),
      Ret;
    {error, Reason} ->
      {error, Reason}
  end.

store_log_position(VBucketUUID, Position) ->
  Tab2 = case ets:file2tab("log_position.dat") of
           {ok, Tab} ->
             Tab;
           {error, _Reason} ->
             ets:new(log_position_table, [])
         end,
  ets:insert(Tab2, {VBucketUUID, Position}),
  ets:tab2file(Tab2, "log_position.dat"),
  ets:delete(Tab2).