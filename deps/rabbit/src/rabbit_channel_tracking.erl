%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_channel_tracking).

%% Abstracts away how tracked connection records are stored
%% and queried.
%%
%% See also:
%%
%%  * rabbit_channel_tracking_handler
%%  * rabbit_reader
%%  * rabbit_event
-behaviour(rabbit_tracking).

-export([boot/0,
         update_tracked/1,
         handle_cast/1,
         register_tracked/1,
         unregister_tracked/1,
         count_tracked_items_in/1,
         clear_tracking_tables/0,
         shutdown_tracked_items/2]).

-export([list/0, list_of_user/1, list_on_node/1,
         delete_tracked_channel_user_entry/1]).

-export([mds_tables/0]).

-include_lib("rabbit_common/include/rabbit.hrl").

-define(TRACKED_CHANNEL_ON_NODE, tracked_channel_on_node).
-define(TRACKED_CHANNEL_PER_USER_ON_NODE, tracked_channel_table_per_user_on_node).

-define(MDS_TABLES, [{?TRACKED_CHANNEL_ON_NODE,
                      #{type => tracking,
                        name => ?TRACKED_CHANNEL_ON_NODE,
                        key => #tracked_channel.id}},
                     {?TRACKED_CHANNEL_PER_USER_ON_NODE,
                      #{type => tracking_counter,
                        name => ?TRACKED_CHANNEL_PER_USER_ON_NODE,
                        key => #tracked_channel_per_user.user,
                        counter => #tracked_channel_per_user.channel_count}}]).

-import(rabbit_misc, [pget/2]).

%%
%% API
%%

%% Sets up and resets channel tracking tables for this node.
-spec boot() -> ok.

boot() ->
    ensure_tracked_channels_table_for_this_node(),
    rabbit_log:info("Setting up a table for channel tracking on this node: ~p",
                    [?TRACKED_CHANNEL_ON_NODE]),
    ensure_per_user_tracked_channels_table_for_node(),
    rabbit_log:info("Setting up a table for channel tracking on this node: ~p",
                    [?TRACKED_CHANNEL_PER_USER_ON_NODE]),
    clear_tracking_tables(),
    ok.

-spec update_tracked(term()) -> ok.

update_tracked(Event) ->
    spawn(?MODULE, handle_cast, [Event]),
    ok.

%% Asynchronously handle update events
-spec handle_cast(term()) -> ok.

handle_cast({channel_created, Details}) ->
    ThisNode = node(),
    case node(pget(pid, Details)) of
        ThisNode ->
            TrackedCh = #tracked_channel{id = TrackedChId} =
                tracked_channel_from_channel_created_event(Details),
            try
                register_tracked(TrackedCh)
            catch
                error:{no_exists, _} ->
                    Msg = "Could not register channel ~p for tracking, "
                          "its table is not ready yet or the channel terminated prematurely",
                    rabbit_log_connection:warning(Msg, [TrackedChId]),
                    ok;
                error:Err ->
                    Msg = "Could not register channel ~p for tracking: ~p",
                    rabbit_log_connection:warning(Msg, [TrackedChId, Err]),
                    ok
            end;
        _OtherNode ->
            %% ignore
            ok
    end;
handle_cast({channel_closed, Details}) ->
    %% channel has terminated, unregister if local
    case get_tracked_channel_by_pid(pget(pid, Details)) of
        [#tracked_channel{name = Name}] ->
            unregister_tracked(rabbit_tracking:id(node(), Name));
        _Other -> ok
    end;
handle_cast({connection_closed, ConnDetails}) ->
    ThisNode= node(),
    ConnPid = pget(pid, ConnDetails),

    case pget(node, ConnDetails) of
        ThisNode ->
            TrackedChs = get_tracked_channels_by_connection_pid(ConnPid),
            rabbit_log_channel:debug(
                "Closing all channels from connection '~s' "
                "because it has been closed", [pget(name, ConnDetails)]),
            %% Shutting down channels will take care of unregistering the
            %% corresponding tracking.
            shutdown_tracked_items(TrackedChs, undefined),
            ok;
        _DifferentNode ->
            ok
    end;
handle_cast({user_deleted, Details}) ->
    Username = pget(name, Details),
    %% Schedule user entry deletion, allowing time for connections to close
    _ = timer:apply_after(?TRACKING_EXECUTION_TIMEOUT, ?MODULE,
            delete_tracked_channel_user_entry, [Username]),
    ok;
handle_cast({node_deleted, Details}) ->
    Node = pget(node, Details),
    rabbit_log_channel:info(
        "Node '~s' was removed from the cluster, deleting"
        " its channel tracking tables...", [Node]),
    delete_tracked_channels_table_for_node(Node),
    delete_per_user_tracked_channels_table_for_node(Node).

-spec register_tracked(rabbit_types:tracked_channel()) -> ok.
-dialyzer([{nowarn_function, [register_tracked/1]}, race_conditions]).

register_tracked(TrackedCh =
  #tracked_channel{node = Node, name = Name, username = Username}) ->
    ChId = rabbit_tracking:id(Node, Name),
    rabbit_store:add_tracked_item(?TRACKED_CHANNEL_ON_NODE,
                                  Node,
                                  ChId,
                                  TrackedCh,
                                  [{?TRACKED_CHANNEL_PER_USER_ON_NODE, Username}]),
    ok.

-spec unregister_tracked(rabbit_types:tracked_channel_id()) -> ok.

unregister_tracked(ChId = {Node, _Name}) when Node =:= node() ->
    rabbit_store:delete_tracked_item(
      ?TRACKED_CHANNEL_ON_NODE, Node, ChId,
      [{?TRACKED_CHANNEL_PER_USER_ON_NODE,
        fun(#tracked_channel{username = Username}) -> Username end}]).

-spec count_tracked_items_in({atom(), rabbit_types:username()}) -> non_neg_integer().

count_tracked_items_in({user, Username}) ->
    rabbit_tracking:count_tracked_items(
        ?TRACKED_CHANNEL_PER_USER_ON_NODE,
        #tracked_channel_per_user.channel_count, Username,
        "channels in vhost").

-spec clear_tracking_tables() -> ok.

clear_tracking_tables() ->
    clear_tracked_channel_tables_for_this_node(),
    ok.

-spec shutdown_tracked_items(list(), term()) -> ok.

shutdown_tracked_items(TrackedItems, _Args) ->
    close_channels(TrackedItems).

%% helper functions
-spec list() -> [rabbit_types:tracked_channel()].

list() ->
    lists:foldl(
      fun (Node, Acc) ->
              try
                  Acc ++
                      rabbit_store:match_tracked_items(?TRACKED_CHANNEL_ON_NODE,
                                                       Node,
                                                       #tracked_channel{_ = '_'})
              catch
                  exit:{aborted, {no_exists, [_, _]}} ->
                      %% The table might not exist yet (or is already gone)
                      %% between the time rabbit_nodes:all_running() runs and
                      %% returns a specific node, and
                      %% mnesia:dirty_match_object() is called for that node's
                      %% table.
                      Acc
              end
      end, [], rabbit_nodes:all_running()).

-spec list_of_user(rabbit_types:username()) -> [rabbit_types:tracked_channel()].

list_of_user(Username) ->
    rabbit_tracking:match_tracked_items(
        ?TRACKED_CHANNEL_ON_NODE,
        #tracked_channel{username = Username, _ = '_'}).

-spec list_on_node(node()) -> [rabbit_types:tracked_channel()].

list_on_node(Node) ->
    try rabbit_store:match_tracked_items(
          ?TRACKED_CHANNEL_ON_NODE,
          Node,
          #tracked_channel{_ = '_'})
    catch exit:{aborted, {no_exists, _}} -> []
    end.

mds_tables() ->
    lists:flatten(
      [[{rabbit_store:tracked_table_name_for(Name, Node), rabbit_store, ExtraArgs#{node => Node}}
        || {Name, ExtraArgs} <- ?MDS_TABLES] || Node <- rabbit_nodes:all_running()]).

%% internal
ensure_tracked_channels_table_for_this_node() ->
    rabbit_store:create_tracking_table(?TRACKED_CHANNEL_ON_NODE, node(), tracked_channel,
                                       record_info(fields, tracked_channel)).

ensure_per_user_tracked_channels_table_for_node() ->
    rabbit_store:create_tracking_table(?TRACKED_CHANNEL_PER_USER_ON_NODE, node(),
                                       tracked_channel_per_user,
                                       record_info(fields, tracked_channel_per_user)).

clear_tracked_channel_tables_for_this_node() ->
    [rabbit_tracking:clear_tracking_table(T, node())
     || T <- get_all_tracked_channel_table_names()].

delete_tracked_channels_table_for_node(Node) ->
    rabbit_tracking:delete_tracking_table(?TRACKED_CHANNEL_ON_NODE, Node, "tracked channel").

delete_per_user_tracked_channels_table_for_node(Node) ->
    rabbit_tracking:delete_tracking_table(?TRACKED_CHANNEL_PER_USER_ON_NODE, Node,
                                       "per-user tracked channels").

get_all_tracked_channel_table_names() ->
    [?TRACKED_CHANNEL_ON_NODE, ?TRACKED_CHANNEL_PER_USER_ON_NODE].

get_tracked_channels_by_connection_pid(ConnPid) ->
    rabbit_tracking:match_tracked_items(
        ?TRACKED_CHANNEL_ON_NODE,
        #tracked_channel{connection = ConnPid, _ = '_'}).

get_tracked_channel_by_pid(ChPid) ->
    rabbit_tracking:match_tracked_items(
        ?TRACKED_CHANNEL_ON_NODE,
        #tracked_channel{pid = ChPid, _ = '_'}).

delete_tracked_channel_user_entry(Username) ->
    rabbit_tracking:delete_tracked_entry(
        {rabbit_auth_backend_internal, exists, [Username]},
        ?TRACKED_CHANNEL_PER_USER_ON_NODE,
        Username).

tracked_channel_from_channel_created_event(ChannelDetails) ->
    Node = node(ChPid = pget(pid, ChannelDetails)),
    Name = pget(name, ChannelDetails),
    #tracked_channel{
        id    = rabbit_tracking:id(Node, Name),
        name  = Name,
        node  = Node,
        vhost = pget(vhost, ChannelDetails),
        pid   = ChPid,
        connection = pget(connection, ChannelDetails),
        username   = pget(user, ChannelDetails)}.

close_channels(TrackedChannels = [#tracked_channel{}|_]) ->
    [rabbit_channel:shutdown(ChPid)
        || #tracked_channel{pid = ChPid} <- TrackedChannels],
    ok;
close_channels(_TrackedChannels = []) -> ok.
