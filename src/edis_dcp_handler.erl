%%%-------------------------------------------------------------------
%%% @author zy
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 23. 七月 2015 11:51 AM
%%%-------------------------------------------------------------------
-module(edis_dcp_handler).
-author("zy").

-behaviour(edcp_producer).
-behaviour(edcp_consumer).

-include_lib("edcp/include/edcp_protocol.hrl").

-export([
    open_stream/3,
    stream_starting/3, stream_snapshot/3, stream_end/1,
    handle_snapshot_item/2, handle_stream_error/2, handle_stream_end/2]).

-compile([{parse_transform, lager_transform}]).

%%%===================================================================
%%% API
%%%===================================================================
open_stream([Host, Port], [VBucketUUID, SeqNoStart, SeqNoEnd], Timeout) ->
    Client = edis_db:process(0),
    edcp_consumer_sup:start([Host, Port], [VBucketUUID, SeqNoStart, SeqNoEnd], Timeout, Client).

%%%===================================================================
%%% edcp_producer callbacks
%%%===================================================================
stream_starting(VBucketUUID, SeqStart, SeqEnd) ->
    lager:debug("start stream with ~p ~p-~p", [VBucketUUID, SeqStart, SeqEnd]),

    case edis_op_logger:open_op_log_file_for_read() of
        none ->
            {error, open_log_file_failed};
        File ->
            SnapShotList = [{SeqStart, SeqEnd}],
            {ok, SnapShotList, File}
    end.

stream_snapshot(SnapshotStart, SnapshotEnd, File) ->
    ItemList = snapshot(File, []),
    {ok, ItemList, File}.

stream_end(_File) ->
    ok.

%%%===================================================================
%%% edcp_consumer callbacks
%%%===================================================================
handle_snapshot_item(Item = {SeqNo, Log}, Client) ->
    {SeqNo, EdisCmd} = edis_op_logger:make_command_from_op_log(Log),
    lager:debug("receive ~p", [EdisCmd]),
    edis_db:run(Client, EdisCmd),
    {ok, Client}.

handle_stream_error(Error, _Client) ->
    lager:debug("stream error ~p", [Error]),
    ok.

handle_stream_end(?Flag_OK, _Client) ->
    lager:debug("stream end normally"),
    ok;
handle_stream_end(ErrorFlag, _Client) ->
    lager:error("stream end with error ~p", [ErrorFlag]),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================
snapshot(File, Items) ->
    case file:read_line(File) of
        {ok, Data} ->
            {OpIndex, Log} = parse_line(Data),
            snapshot(File, [{OpIndex, Log} | Items]);
        _Error ->
            Items
    end.

parse_line(Data) ->
    [OpIndexBin, _Rest] = parse_rest(Data),
    OpIndex = binary_to_integer(OpIndexBin),
    Log = binary:part(Data, {0, size(Data) - 1}),
    {OpIndex, Log}.

parse_rest(Rest) ->
    binary:split(Rest, <<"\\">>).