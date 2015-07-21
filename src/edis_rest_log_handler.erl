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

%% API
-export([]).

%% cowboy exports
-export([init/2]).
-export([content_types_provided/2]).
-export([streaming_csv/2]).

init(Req, State) ->
    {cowboy_rest, Req, State}.

content_types_provided(Req, State) ->
    {[
        {{<<"text">>, <<"csv">>, []}, streaming_csv}
    ], Req, State}.

streaming_csv(Req, State) ->
    File = edis_op_logger:open_op_log_file_for_read(),
    {{stream, result_streamer(State, File)}, Req, State}.

result_streamer(_State, File) ->
    fun (Socket, Transport) ->
        send_records(Socket, Transport, File)
    end.

send_records(Socket, Transport, File) ->
    timer:sleep(500),
    case File of
        none -> send_line(Socket, Transport, <<"">>);
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