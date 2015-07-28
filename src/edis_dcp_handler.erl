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

-record(consumer_state, {edis_client, seq_num}).

%%%===================================================================
%%% API
%%%===================================================================
open_stream([Host, Port], [VBucketUUID, SeqNoStart, SeqNoEnd], Timeout) ->
    Client = edis_db:process(0),
    edcp_consumer_sup:start([Host, Port], [VBucketUUID, SeqNoStart, SeqNoEnd], Timeout, #consumer_state{
        edis_client = Client, seq_num = SeqNoStart - 1
    }).

%%%===================================================================
%%% edcp_producer callbacks
%%%===================================================================
stream_starting(VBucketUUID, SeqStart, SeqEnd) ->
    lager:debug("start stream with ~p ~p-~p", [VBucketUUID, SeqStart, SeqEnd]),

    edis_op_logger:notify_synchronize(self()),

    case edis_op_logger:open_op_log_file_for_read() of
        none ->
            {error, open_log_file_failed};
        File ->
            {ok, File}
    end.

stream_snapshot(SnapshotStart, 0, File) ->
    case get_snapshots(SnapshotStart, 10, File) of
        [] ->
            {hang, File};
        SnapShot ->
            {ok, SnapShot, File}
    end;
stream_snapshot(SnapshotStart, SeqEnd, File) when SeqEnd >= SnapshotStart ->
    SnapshotLen = if
                      SeqEnd - SnapshotStart > 1 -> 2;
                      true -> SeqEnd - SnapshotStart + 1
                  end,
    case get_snapshots(SnapshotStart, SnapshotLen, File) of
        [] ->
            {hang, File};
        SnapShot ->
            {ok, SnapShot, File}
    end.

stream_end(_File) ->
    ok.

%%%===================================================================
%%% edcp_consumer callbacks
%%%===================================================================
handle_snapshot_item({SeqNo, Log}, State = #consumer_state{edis_client = Client}) ->
    {SeqNo, EdisCmd} = edis_op_logger:make_command_from_op_log(Log),
    lager:debug("receive ~p", [EdisCmd]),
    edis_db:run(Client, EdisCmd),
    {ok, State#consumer_state{seq_num = SeqNo}}.

handle_stream_error(Error, #consumer_state{seq_num = SeqNo}) ->
    lager:debug("stream error ~p", [Error]),
    gen_server:cast(edis_dcp_monitor, {stream_error, SeqNo}),
    ok.

handle_stream_end(Flag, #consumer_state{seq_num = SeqNo}) ->
    lager:debug("stream end with flag ~p", [Flag]),
    gen_server:cast(edis_dcp_monitor, {stream_end, SeqNo}),
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================
get_snapshots(StartNum, Len, File) ->
    get_snapshots(StartNum, Len, File, []).

get_snapshots(_StartNum, 0, _File, SnapShot) ->
    lists:reverse(SnapShot);
get_snapshots(StartNum, Len, File, SnapShot) ->
    case get_log(File) of
        file_end ->
            lists:reverse(SnapShot);
        {ok, StartNum, Log} ->
            get_snapshots(StartNum + 1, Len - 1, File, [{StartNum, Log} | SnapShot]);
        {ok, _Index, _Data} ->
            get_snapshots(StartNum, Len, File, SnapShot)
    end.

get_log(File) ->
    case file:read_line(File) of
        {ok, Data} ->
            Index = edis_op_logger:split_index_from_op_log_line(Data),
            Log = binary:part(Data, {0, max(0, size(Data) - 1)}),
            {ok, Index, Log};
        _Error ->
            file_end
    end.