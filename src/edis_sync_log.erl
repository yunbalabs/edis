%%%-------------------------------------------------------------------
%%% @author xuthief
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 03. 八月 2015 下午3:22
%%%-------------------------------------------------------------------
-module(edis_sync_log).
-author("xuthief").

-behaviour(gen_server).
-include("edis.hrl").

%% API
-export([start_link/0, format_command/1, make_command_from_op_log/1]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {}).

-define(OP_LOG_SEP, <<"\\">>).

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
    esync_log:set_sync_receiver(edis_sync_log),
    {ok, #state{}}.

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
handle_info({op_log, {ServerId, Index, BinOpLog}}, State) ->
    try
        Command = make_command_from_op_log(BinOpLog),
        edis_db:sync_command(Command#edis_command.db, Command, ServerId, Index, BinOpLog)
    catch E:T ->
        lager:error("run sync command failed of Log [~p] with exception [~p:~p]", [BinOpLog, E, T])
    end,
    {noreply, State};
handle_info(_Info, State) ->
    lager:info("handle unknown info [~p]", [_Info]),
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

format_command(_Command = #edis_command{timestamp = TimeStamp, db = Db, cmd = Cmd, args = Args, group = Group, result_type
= ResultType}) ->
    iolist_to_binary([
        make_sure_binay(TimeStamp)
        , ?OP_LOG_SEP, make_sure_binay(Db)
        , ?OP_LOG_SEP, make_sure_binay(Cmd)
        , ?OP_LOG_SEP, make_sure_binay(Group)
        , ?OP_LOG_SEP, make_sure_binay(ResultType)
    ] ++ lists:map(fun(E) -> iolist_to_binary([?OP_LOG_SEP, make_sure_binay(E)]) end, Args)
).

make_command_from_op_log(BinOpLog) ->
    [BinTimeStamp, Bin2] = binary:split(BinOpLog, ?OP_LOG_SEP),
    [BinDb, Bin3] = binary:split(Bin2, ?OP_LOG_SEP),
    [BinCmd, Bin4] = binary:split(Bin3, ?OP_LOG_SEP),
    [BinGroup, Bin5] = binary:split(Bin4, ?OP_LOG_SEP),
    [BinResultType, Bin6] = binary:split(Bin5, ?OP_LOG_SEP),
    Args = binary:split(Bin6, ?OP_LOG_SEP, [global]),

    #edis_command{
        timestamp = binary_to_float(BinTimeStamp),
        db = binary_to_integer(BinDb),
        cmd = BinCmd,
        group = binary_to_atom(BinGroup, latin1),
        result_type = binary_to_atom(BinResultType, latin1),
        args = Args
    }.

make_sure_binay(Data) ->
    if
        is_integer(Data) ->
            integer_to_binary(Data);
        is_list(Data) ->
            list_to_binary(Data);
        is_atom(Data) ->
            atom_to_binary(Data, latin1);
        is_float(Data) ->
            float_to_binary(Data);
        true ->
            Data
    end.

