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

-export([init/3]).
-export([rest_init/2]).
-export([content_types_provided/2]).
-export([streaming_csv/2]).

-define(DEFAULT_REQ_INDEX, 0).

init(_Transport, _Req, _Table) ->
        {upgrade, protocol, cowboy_rest}.

rest_init(Req, Table) ->
        {ok, Req, Table}.

content_types_provided(Req, State) ->
        {[
                {{<<"text">>, <<"csv">>, []}, streaming_csv}
        ], Req, State}.

streaming_csv(Req, State) ->
    Index = get_req_index(Req),
    File = edis_op_logger:open_op_log_file_for_read(),
    position_file_to_index(File, Index),
    {{stream, result_streamer(State, File)}, Req, State}.

result_streamer(_State, File) ->
    fun (Socket, Transport) ->
        send_records(Socket, Transport, File)
    end.

send_records(Socket, Transport, File) ->
    timer:sleep(500),
    case File of
        none -> send_line(Socket, Transport, <<"">>),
        ok;
        _ ->
            case file:read_line(File) of
                {ok, Data} ->
                    send_line(Socket, Transport, Data),
                    send_records(Socket, Transport, File);
                eof ->
                    lager:error("read_line finished with eof", []),
                    ok;
                {error, eof} ->
                    lager:error("read_line finished with {error, eof}", []),
                    ok;
                Error ->
                    lager:error("read_line failed with error [~p]", [Error])
            end
    end.

send_line(Socket, Transport, OpLine) ->
    Transport:send(Socket,
        [OpLine, $\r, $\n]).

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
