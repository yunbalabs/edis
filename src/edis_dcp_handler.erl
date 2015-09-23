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
-include_lib("esync_log/include/esync_log.hrl").

-export([
    open_stream/2,
    stream_starting/3, stream_snapshot/3, stream_end/1, stream_info/2,
    handle_snapshot_marker/3, handle_snapshot_item/2, handle_stream_error/2, handle_stream_end/2]).

-compile([{parse_transform, lager_transform}]).

-record(consumer_state, {seq_num, server_id}).

%%%===================================================================
%%% API
%%%===================================================================
open_stream([Host, Port], [VBucketUUID, SeqNoStart, SeqNoEnd]) ->
    ServerId = edis_config:get(server_id),
    edcp_consumer_sup:start([Host, Port], [VBucketUUID, SeqNoStart, SeqNoEnd], #consumer_state{
        seq_num = SeqNoStart - 1, server_id = ServerId
    }).

%%%===================================================================
%%% edcp_producer callbacks
%%%===================================================================
stream_starting(VBucketUUID, SeqStart, SeqEnd) ->
    lager:debug("start stream with ~p ~p-~p", [VBucketUUID, SeqStart, SeqEnd]),

    edis_op_logger:notify_synchronize(self()),

    case esync_log_op_logger:open_read_logger() of
        none ->
            {error, open_log_file_failed};
        File ->
            {ok, File}
    end.

stream_snapshot(SnapshotStart, 0, File) ->
    case get_snapshots(SnapshotStart, 10, File) of
        [] ->
            {stop, File};
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
            {stop, File};
        SnapShot ->
            {ok, SnapShot, File}
    end.

stream_end(_File) ->
    ok.

stream_info(_Info, ModState) ->
    {ok, ModState}.

%%%===================================================================
%%% edcp_consumer callbacks
%%%===================================================================
handle_snapshot_marker(SnapshotStart, _SnapshotEnd, ModState) ->
    gen_server:cast(edis_dcp_monitor, {snapshot_marker, SnapshotStart}),
    {ok, ModState}.

handle_snapshot_item({SeqNo, Log}, State = #consumer_state{server_id = SId}) ->
    [ServerId, Rest1] = binary:split(Log, ?OP_LOG_SEP),
    BinServerId = integer_to_binary(SId),
    lager:debug("handle log ~p ~p from server_id ~p", [SeqNo, Log, ServerId]),
    case ServerId of
        BinServerId ->
            lager:debug("server id equal with log server id [~p], ignore [~p]", [ServerId, Log]),
            {ok, State#consumer_state{seq_num = SeqNo}};
        _ ->
            [_Index, Rest2] = binary:split(Rest1, ?OP_LOG_SEP),
            edis_op_logger:sync_log(Rest2),
            {ok, State#consumer_state{seq_num = SeqNo}}
    end.

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
        {ok, Index, Log} when Index >= StartNum ->
            get_snapshots(Index, Len - 1, File, [{StartNum, Log} | SnapShot]);
        {ok, _Index, _Data} ->
            get_snapshots(StartNum, Len, File, SnapShot)
    end.

get_log(File) ->
    case file:read_line(File) of
        {ok, Data} ->
            Index = esync_log_op_logger:get_line_index(Data),
            Log = binary:part(Data, {0, max(0, size(Data) - 1)}),
            {ok, Index, Log};
        _Error ->
            file_end
    end.

get_server_id() ->
    FileName = ?DEFAULT_SERVER_ID_FILE_NAME,
    case file:read_file(FileName) of
        {ok, ServerId} ->
            lager:debug("got server id from file [~p]", [ServerId]),
            ServerId;
        {error, Error} ->
            lager:info("open server id file [~p] failed [~p], recreate it!", [FileName, Error]),
            ServerId = gen_server_id(),
            case file:write_file(FileName, ServerId, [exclusive, raw, binary]) of
                ok ->
                    lager:info("create server id file [~p] succ", [FileName]),
                    ok;
                Error ->
                    lager:error("create server id file [~p] failed [~P]", [FileName, Error])
            end,
            ServerId
    end.

gen_server_id() ->
    %% generated server id is based on nodename & ip & timestamp
    HashInt = erlang:phash2([node(), inet:getif(), os:timestamp()]),
    integer_to_binary(HashInt).