%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_tracking).

%% Common behaviour and processing functions for tracking components
%%
%% See in use:
%%  * rabbit_connection_tracking
%%  * rabbit_channel_tracking

-callback boot() -> ok.
-callback update_tracked(term()) -> ok.
-callback handle_cast(term()) -> ok.
-callback register_tracked(
              rabbit_types:tracked_connection() |
                  rabbit_types:tracked_channel()) -> 'ok'.
-callback unregister_tracked(
              rabbit_types:tracked_connection_id() |
                  rabbit_types:tracked_channel_id()) -> 'ok'.
-callback count_tracked_items_in(term()) -> non_neg_integer().
-callback clear_tracking_tables() -> 'ok'.
-callback shutdown_tracked_items(list(), term()) -> ok.

-export([id/2, count_tracked_items/4, match_tracked_items/2,
         clear_tracking_table/2, delete_tracking_table/3,
         delete_tracked_entry/3]).

%%----------------------------------------------------------------------------

-spec id(atom(), term()) ->
    rabbit_types:tracked_connection_id() | rabbit_types:tracked_channel_id().

id(Node, Name) -> {Node, Name}.

-spec count_tracked_items(function(), integer(), term(), string()) ->
    non_neg_integer().

count_tracked_items(Name, CountRecPosition, Key, ContextMsg) ->
    lists:foldl(fun (Node, Acc) ->
                        try
                            N = case rabbit_store:lookup_tracked_item(Name, Node, Key) of
                                    []    -> 0;
                                    [Val] when is_integer(Val) -> Val;
                                    [Val] -> element(CountRecPosition, Val)
                                end,
                            Acc + N
                        catch _:Err  ->
                                rabbit_log:error(
                                  "Failed to fetch number of ~p ~p on node ~p:~n~p",
                                  [ContextMsg, Key, Node, Err]),
                                Acc
                        end
                end, 0, rabbit_nodes:all_running()).

-spec match_tracked_items(function(), tuple()) -> term().

match_tracked_items(Name, MatchSpec) ->
    lists:foldl(
        fun (Node, Acc) ->
                Acc ++ rabbit_store:match_tracked_items(Name, Node, MatchSpec)
        end, [], rabbit_nodes:all_running()).

-spec clear_tracking_table(atom(), atom()) -> ok.

clear_tracking_table(Name, Node) ->
    rabbit_store:clear_tracking_table(Name, Node).

-spec delete_tracking_table(atom(), node(), string()) -> ok.

delete_tracking_table(Name, Node, ContextMsg) ->
    rabbit_store:delete_tracking_table(Name, Node, ContextMsg).

-spec delete_tracked_entry({atom(), atom(), list()}, function(), term()) -> ok.

delete_tracked_entry(_ExistsCheckSpec = {M, F, A}, TableName, Key) ->
    ClusterNodes = rabbit_nodes:all_running(),
    ExistsInCluster =
        lists:any(fun(Node) -> rpc:call(Node, M, F, A) end, ClusterNodes),
    case ExistsInCluster of
        false ->
            [rabbit_store:delete_tracked_entry(TableName, Node, Key) || Node <- ClusterNodes];
        true ->
            ok
    end.
