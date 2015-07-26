%%%-------------------------------------------------------------------
%%% @author thi
%%% @copyright (C) 2015, <YUNBA.IO>
%%% @doc
%%%
%%% @end
%%% Created : 21. 七月 2015 下午3:10
%%%-------------------------------------------------------------------
-module(edis_rest_log_handler).
-author("thi").
-define(DEFAULT_REQ_INDEX, 0).

-behaviour(cowboy_loop_handler).

-export([init/3]).
-export([info/3]).
-export([terminate/3]).

-record(state, {
    file			:: term()
}).

init(_, Req, _) ->
    self() ! response,
    {loop, Req, #state{}, hibernate}.

info(response, Req, State) ->
    Index = get_req_index(Req),
    File = edis_op_logger:open_op_log_file_for_read(),
    position_file_to_index(File, Index),
    Req2 = cowboy_req:set([{resp_state, waiting_stream}], Req),
    {ok, Req3} = cowboy_req:chunked_reply(200, Req2),
    trigger_send_line(),
    {loop, Req3, State#state{file = File}};

info(send_line, Req, State = #state{file = File}) ->
    case send_line(Req, File) of
        ok ->
            trigger_send_line(),
            {loop, Req, State};
        _ ->
            {ok, Req, State}
    end.

terminate(Reason, _, _) ->
    lager:debug("Req terminate by [~p]", [Reason]),
    ok.

trigger_send_line() ->
    self() ! send_line.

send_line(Req, File) ->
    timer:sleep(500),
    case File of
        none -> error;
        _ ->
            case file:read_line(File) of
                {ok, Data} ->
                    Line = binary:part(Data, 0, max(0, byte_size(Data)-1)),
                    cowboy_req:chunk(Line, Req),
                    ok;
                eof ->
                    %lager:error("read_line finished with eof", []),
                    ok;
                {error, eof} ->
                    %lager:error("read_line finished with {error, eof}", []),
                    ok;
                Error ->
                    lager:error("read_line failed with error [~p]", [Error]),
                    file:close(File),
                    error
            end
    end.

get_req_index(Req) ->
    {_Method, Req2} = cowboy_req:method(Req),
    {BinIndex, _Req3} = cowboy_req:qs_val(<<"index">>, Req2),
    lager:debug("Index [~p]", [BinIndex]),
    case BinIndex of
        <<"undefined">> ->
            lager:info("index argument not found, set to default index [~p]", [?DEFAULT_REQ_INDEX]),
            ?DEFAULT_REQ_INDEX;
        _ ->
            try
                binary_to_integer(BinIndex)
            catch E:T ->
                lager:error("parge binary index [~p] failed [~p:~p], set default index [~p]", [BinIndex, E, T, ?DEFAULT_REQ_INDEX]),
                ?DEFAULT_REQ_INDEX
            end
    end.

position_file_to_index(File, Index) ->
    case File of
        none -> ok;
        _ ->
            case Index of
                0 ->
                    file:position(File, {bof, 0});
                _ ->
                    position_line_by_line(File, Index)
            end
    end,
    File.

position_line_by_line(File, Index) ->
    case file:read_line(File) of
        {ok, Line} ->
            case edis_op_logger:split_index_from_op_log_line(Line) of
                Index ->
                    lager:info("found index [~p] in Line [~p]", [Index, Line]),
                    ok;
                N when N < Index ->
                    position_line_by_line(File, Index);
                N ->
                    lager:error("splited index [~p] > specified index [~p], set back to previos line [~p]", [N, Index, Line]),
                    {ok, _} = file:position(File, {cur, -byte_size(Line)}),
                    ok
            end
    end.
