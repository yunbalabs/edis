%%%-------------------------------------------------------------------
%%% @author zy
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 04. 八月 2015 4:22 PM
%%%-------------------------------------------------------------------
-module(edis_rest_config_handler).
-author("zy").

%% API
-export([init/3, handle/2]).

-record(state, {action ::term()}).

init(_Transport, Req, _Opts) ->
    case cowboy_req:method(Req) of
        {<<"POST">>, _} ->
            {shutdown, Req, undefined};
        {<<"GET">>, Req1} ->
            Req2 = enable_cors(Req1),
            {PathInfo, _} = cowboy_req:path_info(Req2),
            case PathInfo of
                [<<"log">>, <<"disable">>] ->
                    {ok, Req2, #state{action = disable_transaction}};
                [<<"log">>, <<"enable">>] ->
                    {ok, Req2, #state{action = enable_transaction}};
                _ ->
                    {shutdown, Req2, undefined}
            end
    end.

handle(Req, HttpState = #state{action = disable_transaction}) ->
    case cowboy_req:qs_val(<<"transaction">>, Req) of
        {Transaction, _} when is_binary(Transaction) ->
            edis_op_logger:disable_transaction(Transaction),
            reply_success(Req, HttpState);
        _ ->
            reply_failed(Req, HttpState)
    end;
handle(Req, HttpState = #state{action = enable_transaction}) ->
    case cowboy_req:qs_val(<<"transaction">>, Req) of
        {Transaction, _} when is_binary(Transaction) ->
            edis_op_logger:enable_transaction(Transaction),
            reply_success(Req, HttpState);
        _ ->
            reply_failed(Req, HttpState)
    end.

reply_success(Req, HttpState) ->
    {ok, Req1} = cowboy_req:reply(200, json_headers(), jiffy:encode({[{<<"status">>, <<"success">>}]}), Req),
    {ok, Req1, HttpState}.

reply_failed(Req, HttpState) ->
    {ok, Req1} = cowboy_req:reply(200, json_headers(), jiffy:encode({[{<<"status">>, <<"failed">>}]}), Req),
    {ok, Req1, HttpState}.

json_headers() ->
    [{<<"content-type">>, <<"application/json">>}].

enable_cors(Req) ->
    case cowboy_req:header(<<"origin">>, Req) of
        {undefined, _} ->
            Req;
        {Origin, _} ->
            Req1 = cowboy_req:set_resp_header(<<"access-control-allow-origin">>, Origin, Req),
            cowboy_req:set_resp_header(<<"access-control-allow-credentials">>, <<"true">>, Req1)
    end.