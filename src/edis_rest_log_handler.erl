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

-behaviour(cowboy_http_handler).
-export([init/3, handle/2, terminate/3]).

init({_Transport, http}, Req, _Opts) ->
    {ok, Req, undefined}.

handle(Req, State) ->
    Req2 = cowboy_req:set([{resp_state, waiting_stream}], Req),
    {ok, Req3} = cowboy_req:chunked_reply(200, Req2),

    Index = get_req_index(Req),
    File = edis_op_logger:open_op_log_file_for_read(),
    position_file_to_index(File, Index),

    send_records(Req3, File),
    {ok, Req3, State}.

terminate(_, _, _) ->
    ok.

send_records(Req, File) ->
    timer:sleep(500),
    case File of
        none -> send_line(Req, <<"">>),
        ok;
        _ ->
            case file:read_line(File) of
                {ok, Data} ->
                    Line = binary:part(Data, 0, max(0, byte_size(Data)-1)),
                    send_line(Req, Line),
                    send_records(Req, File);
                eof ->
                    %lager:error("read_line finished with eof", []),
                    send_records(Req, File);
                {error, eof} ->
                    %lager:error("read_line finished with {error, eof}", []),
                    send_records(Req, File);
                Error ->
                    lager:error("read_line failed with error [~p]", [Error]),
                    ok
            end
    end.

send_line(Req, OpLine) ->
    cowboy_req:chunk(OpLine, Req).

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
