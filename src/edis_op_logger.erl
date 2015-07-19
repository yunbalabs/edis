%%%-------------------------------------------------------------------
%%% @author xuthief
%%% @copyright (C) 2015, <YUNBA.IO>
%%% @doc
%%%
%%% @end
%%% Created : 19. 七月 2015 下午8:54
%%%-------------------------------------------------------------------
-module(edis_op_logger).
-author("xuthief").

-include("edis.hrl").
-behaviour(gen_event).

%% API
-export([start_link/0,
    add_handler/0]).

-export([log_command/1]).

%% gen_event callbacks
-export([init/1,
    handle_event/2,
    handle_call/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
    op_index,
    server_id
}).

%%%===================================================================
%%% gen_event callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates an event manager
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link() -> {ok, pid()} | {error, {already_started, pid()}}).
start_link() ->
    gen_event:start_link({local, ?SERVER}).

%%--------------------------------------------------------------------
%% @doc
%% Adds an event handler
%%
%% @end
%%--------------------------------------------------------------------
-spec(add_handler() -> ok | {'EXIT', Reason :: term()} | term()).
add_handler() ->
    gen_event:add_handler(?SERVER, ?MODULE, []).


%% @doc Notifies an op log.
-spec log_command(#edis_command{}) -> ok.
log_command(Command) ->
    gen_event:notify(?MODULE, {oplog, Command}).

%%%===================================================================
%%% gen_event callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a new event handler is added to an event manager,
%% this function is called to initialize the event handler.
%%
%% @end
%%--------------------------------------------------------------------
-spec(init(InitArgs :: term()) ->
    {ok, State :: #state{}} |
    {ok, State :: #state{}, hibernate} |
    {error, Reason :: term()}).
-define(DEFAULT_LOG_IDX_FILE, "priv/op_log.idx").
-define(DEFAULT_START_OP_INDEX, 0).
-define(DEFAULT_SERVER_ID, "server1").
init([]) ->
    %% get start op log index from file
    StartOpIndex =
        case file:read_file(?DEFAULT_LOG_IDX_FILE) of
            {ok, File} ->
                try
                    erlang:binary_to_integer(File)
                catch E:T ->
                    lager:error("binary to integer [~p] failed of [~p:~p]", [File, E, T]),
                    ?DEFAULT_START_OP_INDEX
                end;
            {error, enoent} ->
                lager:info("no op log idx file found, deault to index 0", []),
                ?DEFAULT_START_OP_INDEX;
            Error ->
                lager:info("read log idx file failed with error [~p]", [Error]),
                ?DEFAULT_START_OP_INDEX
        end,

    %% get server id
    ServerId = ?DEFAULT_SERVER_ID,

    {ok, #state{op_index = StartOpIndex, server_id = ServerId}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever an event manager receives an event sent using
%% gen_event:notify/2 or gen_event:sync_notify/2, this function is
%% called for each installed event handler to handle the event.
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_event(Event :: term(), State :: #state{}) ->
    {ok, NewState :: #state{}} |
    {ok, NewState :: #state{}, hibernate} |
    {swap_handler, Args1 :: term(), NewState :: #state{},
        Handler2 :: (atom() | {atom(), Id :: term()}), Args2 :: term()} |
    remove_handler).

handle_event({oplog, Command#edis_command{}}, State#state{op_index = OpIndex}) ->
    BinOpLog = format_command_to_op_log(OpIndex, Command),
    write_bin_log_to_file(BinOpLog),
    {ok, State};

handle_event(_Event, State) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever an event manager receives a request sent using
%% gen_event:call/3,4, this function is called for the specified
%% event handler to handle the request.
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), State :: #state{}) ->
    {ok, Reply :: term(), NewState :: #state{}} |
    {ok, Reply :: term(), NewState :: #state{}, hibernate} |
    {swap_handler, Reply :: term(), Args1 :: term(), NewState :: #state{},
        Handler2 :: (atom() | {atom(), Id :: term()}), Args2 :: term()} |
    {remove_handler, Reply :: term()}).
handle_call(_Request, State) ->
    Reply = ok,
    {ok, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called for each installed event handler when
%% an event manager receives any other message than an event or a
%% synchronous request (or a system message).
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: term(), State :: #state{}) ->
    {ok, NewState :: #state{}} |
    {ok, NewState :: #state{}, hibernate} |
    {swap_handler, Args1 :: term(), NewState :: #state{},
        Handler2 :: (atom() | {atom(), Id :: term()}), Args2 :: term()} |
    remove_handler).
handle_info(_Info, State) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever an event handler is deleted from an event manager, this
%% function is called. It should be the opposite of Module:init/1 and
%% do any necessary cleaning up.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Args :: (term() | {stop, Reason :: term()} | stop |
remove_handler | {error, {'EXIT', Reason :: term()}} |
{error, term()}), State :: term()) -> term()).
terminate(_Arg, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
    {ok, NewState :: #state{}}).
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-define(OP_LOG_SEP, <<"\\">>).
format_command_to_op_log(OpIndex, Command#edis_command{timestamp = TimeStamp, db = Db, cmd = Cmd, args = Args}) ->
    iolist_to_binary([make_sure_binay(OpIndex),
        ?OP_LOG_SEP, TimeStamp,
        ?OP_LOG_SEP, Db,
        ?OP_LOG_SEP, Cmd,
        ?OP_LOG_SEP] ++ lists:foreach(Args, fun(E) -> make_sure_binay(E) end)
    ).

make_sure_binay(Data) ->
    if
        is_integer(Data) ->
            integer_to_binary(Data);
        is_list(Data) ->
            list_to_binary(Data);
        is_atom(Data) ->
            atom_to_binary(Data, latin1);
        true ->
            Data
    end.