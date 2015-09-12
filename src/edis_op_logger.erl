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

-export([log_command/1, notify_synchronize/1, disable_transaction/1, enable_transaction/1]).

%% gen_event callbacks
-export([init/1,
    handle_event/2,
    handle_call/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-define(DEFAULT_SERVER_ID, 0).
-define(OP_LOG_SEP, <<"\\">>).

-record(state, {
        server_id       =   ?DEFAULT_SERVER_ID      ::integer(),
        synchronize_pid = undefined                 ::pid(),
        transaction_filter_table                    ::term(),
        db_client                                   ::pid()
}).

-type esync_element_op() :: sadd | srem | expire.
-record(esync_command, {
    timestamp       :: float(),
    element_op      :: esync_element_op(),
    db = 0          :: non_neg_integer(),
    key = <<>>      :: binary(),
    element         :: term(),
    cvs             :: binary()
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
    Server = gen_event:start_link({local, ?SERVER}),
    add_handler(),
    Server.

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

notify_synchronize(Pid) ->
    gen_event:notify(?MODULE, {synchronize, Pid}).

disable_transaction(Transaction) when is_binary(Transaction) ->
    gen_event:notify(?MODULE, {insert_transaction, Transaction}).

enable_transaction(Transaction) when is_binary(Transaction) ->
    gen_event:notify(?MODULE, {remove_transaction, Transaction}).

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

init([]) ->
    %% get server id
    ServerId = ?DEFAULT_SERVER_ID,

    %% set self as sync receiver & get a client to store/query VC
    esync_log:set_sync_receiver(edis_sync_log),
    DbClient = edis_db:process(0),

    FilterTables = sets:new(),

    {ok, #state{server_id = ServerId, transaction_filter_table = FilterTables, db_client = DbClient}}.

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
handle_event({oplog, Command = #edis_command{}}, State = #state{
    server_id = ServerId, db_client = DbClient, synchronize_pid = SyncPid}) ->
    case format_command(Command) of
        {ok, FormatCommand, Elements} ->
            BinLogs = lists:map(
                fun (Element) ->
                    CvsCommand = get_cvs(DbClient, ServerId, FormatCommand#esync_command{element = Element}),
                    make_bin_log_from_format_command(CvsCommand)
                end, Elements),
            esync_log:log_command(BinLogs);
        none ->
            ok
    end,
    case is_process_alive(SyncPid) of
        true ->
            {ok, State};
        _ ->
            {ok, State#state{synchronize_pid = undefined}}
    end;

handle_event({synchronize, Pid}, State) ->
    {ok, State#state{synchronize_pid = Pid}};

handle_event({insert_transaction, Transaction}, State = #state{transaction_filter_table = FilterTable}) ->
    FilterTable2 = sets:add_element(Transaction, FilterTable),
    {ok, State#state{transaction_filter_table = FilterTable2}};

handle_event({remove_transaction, Transaction}, State = #state{transaction_filter_table = FilterTable}) ->
    FilterTable2 = sets:del_element(Transaction, FilterTable),
    {ok, State#state{transaction_filter_table = FilterTable2}};

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
terminate(_Arg, _State = #state{}) ->
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
make_command_from_op_log(BinOpLog) ->
    [BinOpIndex, Bin1] = binary:split(BinOpLog, ?OP_LOG_SEP),
    [BinTimeStamp, Bin2] = binary:split(Bin1, ?OP_LOG_SEP),
    [BinDb, Bin3] = binary:split(Bin2, ?OP_LOG_SEP),
    [BinCmd, Bin4] = binary:split(Bin3, ?OP_LOG_SEP),
    [BinGroup, Bin5] = binary:split(Bin4, ?OP_LOG_SEP),
    {BinResultType, Args} = case binary:split(Bin5, ?OP_LOG_SEP) of
                                [BinResultType2, Bin6] ->
                                    Args2 = binary:split(Bin6, ?OP_LOG_SEP, [global]),
                                    {BinResultType2, Args2};
                                [BinResultType2] ->
                                    {BinResultType2, []}
                            end,

    {binary_to_integer(BinOpIndex),
        #edis_command{
            timestamp = binary_to_integer(BinTimeStamp) + 0.0,
            db = binary_to_integer(BinDb),
            cmd = BinCmd,
            group = binary_to_atom(BinGroup, latin1),
            result_type = binary_to_atom(BinResultType, latin1),
            args = Args
    }}.

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

format_command(_Command = #edis_command{timestamp = TimeStamp, db = Db, cmd = Cmd, args = Args, group = _Group, result_type
= _ResultType}) ->
    case Cmd of
        <<"SADD">> when length(Args)>1 ->
            {[Key], Elements} = lists:split(Args, 1),
            {ok, #esync_command{timestamp = TimeStamp, element_op = sadd, db = Db, key = Key}, Elements};
        <<"SREM">> when length(Args)>1 ->
            {[Key], Elements} = lists:split(Args, 1),
            {ok, #esync_command{timestamp = TimeStamp, element_op = srem, db = Db, key = Key}, Elements};
        <<"EXPIRE">> when length(Args)==2 ->
            {[Key], [Expire]} = lists:split(Args, 1),
            {ok, #esync_command{timestamp = TimeStamp, element_op = srem, db = Db, key = Key}, [Expire]};
        _ ->
            lager:debug("not logged command ~p", [Cmd]),
            none
    end.

-define(CVS_BIT_SIZE, 15).
-define(DEFAULT_CVS, <<0:?CVS_BIT_SIZE>>).

get_cvs(DbClient, ServerId, ElementCommand = #esync_command{timestamp = Timestamp, key = Key, element = Element, element_op = Op, db = Db}) ->
    CvsKey = lists:concat([Key, "_", Element]),
    OldCvsCommand = #edis_command{
        timestamp = Timestamp,
        cmd = <<"GET">>,
        db = Db,
        args = [CvsKey],
        group = keys,
        result_type = number,
        timeout = undefined,
        expire = undefined
    },
    OldCvs =
        try
            edis_db:run_no_oplog(DbClient, OldCvsCommand)
        catch E:T ->
            lager:error("get old cvs failed for Cvskey ~p ~p:~p", [CvsKey, E ,T]),
            ?DEFAULT_CVS
        end,
    KeptCvsBitSize = ?CVS_BIT_SIZE-1,
    <<_:1, OldKeptCvs:KeptCvsBitSize>> = OldCvs,
    NewCvs = case Op of
                 sadd -> <<1:1, OldKeptCvs:KeptCvsBitSize>>;
                 srem -> <<1:0, OldCvs:KeptCvsBitSize>>;
                 _ -> <<1:1, OldKeptCvs:KeptCvsBitSize>>
             end,
    ElementCommand#esync_command{cvs = NewCvs}.

-define(SEP, <<"\\">>).
make_bin_log_from_format_command(_CvsCommand = #esync_command{timestamp = Timestamp, element_op = Op, db = Db, key = Key, element = Element, cvs = Cvs}) ->
    TruncTimestamp = trunc(Timestamp),
    iolist_to_binary([
        make_sure_binay(TruncTimestamp)
        , ?SEP, make_sure_binay(Db)
        , ?SEP, make_sure_binay(Key)
        , ?SEP, make_sure_binay(Element)
        , ?SEP, make_sure_binay(Cvs)
        , ?SEP, make_sure_binay(Timestamp)
    ]).
