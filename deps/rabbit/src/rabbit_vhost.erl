%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_vhost).

-include_lib("rabbit_common/include/rabbit.hrl").

-include_lib("khepri/include/khepri.hrl").

-include("vhost.hrl").

-export([recover/0, recover/1, read_config/1]).
-export([add/2, add/4, delete/2, exists/1,
         with/2, with_in_mnesia/2, with_in_khepri/2,
         with_user_and_vhost/3, with_user_and_vhost_in_mnesia/3, with_user_and_vhost_in_khepri/3,
         assert/1, update/2,
         set_limits/2, vhost_cluster_state/1, is_running_on_all_nodes/1, await_running_on_all_nodes/2,
        list/0, count/0, list_names/0, all/0, all_tagged_with/1]).
-export([parse_tags/1, update_metadata/2, tag_with/2, untag_from/2, update_tags/2, update_tags/3]).
-export([lookup/1]).
-export([info/1, info/2, info_all/0, info_all/1, info_all/2, info_all/3]).
-export([dir/1, msg_store_dir_path/1, msg_store_dir_wildcard/0, config_file_path/1, ensure_config_file/1]).
-export([delete_storage/1]).
-export([vhost_down/1]).
-export([put_vhost/5]).
-export([clear_data_in_khepri/0,
         mnesia_write_to_khepri/1,
         mnesia_delete_to_khepri/1]).
-export([khepri_vhosts_path/0,
         khepri_vhost_path/1]).

-ifdef(TEST).
-export([do_add_to_mnesia/3,
         do_add_to_khepri/3,
         lookup_in_mnesia/1,
         lookup_in_khepri/1,
         exists_in_mnesia/1,
         exists_in_khepri/1,
         list_names_in_mnesia/0,
         list_names_in_khepri/0,
         all_in_mnesia/0,
         all_in_khepri/0,
         update_in_mnesia/2,
         update_in_khepri/2,
         update_in_mnesia/3,
         update_in_khepri/3,
         info_in_mnesia/1,
         info_in_khepri/1,
         clear_permissions_in_mnesia/2,
         clear_permissions_in_khepri/2,
         internal_delete_in_mnesia/1,
         internal_delete_in_khepri/1]).
-endif.

%%
%% API
%%

-type vhost_tag() :: atom() | string() | binary().
-export_type([vhost_tag/0]).

recover() ->
    %% Clear out remnants of old incarnation, in case we restarted
    %% faster than other nodes handled DOWN messages from us.
    rabbit_amqqueue:on_node_down(node()),

    rabbit_amqqueue:warn_file_limit(),

    %% Prepare rabbit_semi_durable_route table
    {Time, _} = timer:tc(fun() ->
                                 rabbit_binding:recover()
                         end),
    rabbit_log:debug("rabbit_binding:recover/0 completed in ~fs", [Time/1000000]),

    %% rabbit_vhost_sup_sup will start the actual recovery.
    %% So recovery will be run every time a vhost supervisor is restarted.
    ok = rabbit_vhost_sup_sup:start(),

    [ok = rabbit_vhost_sup_sup:init_vhost(VHost) || VHost <- list_names()],
    ok.

recover(VHost) ->
    VHostDir = msg_store_dir_path(VHost),
    rabbit_log:info("Making sure data directory '~ts' for vhost '~s' exists",
                    [VHostDir, VHost]),
    VHostStubFile = filename:join(VHostDir, ".vhost"),
    ok = rabbit_file:ensure_dir(VHostStubFile),
    ok = file:write_file(VHostStubFile, VHost),
    ok = ensure_config_file(VHost),
    {Recovered, Failed} = rabbit_amqqueue:recover(VHost),
    AllQs = Recovered ++ Failed,
    QNames = [amqqueue:get_name(Q) || Q <- AllQs],
    {Time, ok} = timer:tc(fun() ->
                                  rabbit_binding:recover(rabbit_exchange:recover(VHost), QNames)
                          end),
    rabbit_log:debug("rabbit_binding:recover/2 for vhost ~s completed in ~fs", [VHost, Time/1000000]),

    ok = rabbit_amqqueue:start(Recovered),
    %% Start queue mirrors.
    ok = rabbit_mirror_queue_misc:on_vhost_up(VHost),
    ok.

ensure_config_file(VHost) ->
    Path = config_file_path(VHost),
    case filelib:is_regular(Path) of
        %% The config file exists. Do nothing.
        true ->
            ok;
        %% The config file does not exist.
        %% Check if there are queues in this vhost.
        false ->
            QueueDirs = rabbit_queue_index:all_queue_directory_names(VHost),
            SegmentEntryCount = case QueueDirs of
                %% There are no queues. Write the configured value for
                %% the segment entry count, or the new RabbitMQ default
                %% introduced in v3.8.17. The new default provides much
                %% better memory footprint when many queues are used.
                [] ->
                    application:get_env(rabbit, queue_index_segment_entry_count,
                        2048);
                %% There are queues already. Write the historic RabbitMQ
                %% default of 16384 for forward compatibility. Historic
                %% default calculated as trunc(math:pow(2,?REL_SEQ_BITS)).
                _ ->
                    ?LEGACY_INDEX_SEGMENT_ENTRY_COUNT
            end,
            rabbit_log:info("Setting segment_entry_count for vhost '~s' with ~b queues to '~b'",
                            [VHost, length(QueueDirs), SegmentEntryCount]),
            file:write_file(Path, io_lib:format(
                "%% This file is auto-generated! Edit at your own risk!~n"
                "{segment_entry_count, ~b}.",
                [SegmentEntryCount]))
    end.

read_config(VHost) ->
    Config = case file:consult(config_file_path(VHost)) of
        {ok, Val}       -> Val;
        %% the file does not exist yet, likely due to an upgrade from a pre-3.7
        %% message store layout so use the history default.
        {error, _}      -> #{
            segment_entry_count => ?LEGACY_INDEX_SEGMENT_ENTRY_COUNT
        }
    end,
    rabbit_data_coercion:to_map(Config).

-define(INFO_KEYS, vhost:info_keys()).

-spec parse_tags(binary() | string() | atom()) -> [atom()].
parse_tags(undefined) ->
    [];
parse_tags(<<"">>) ->
    [];
parse_tags([]) ->
    [];
parse_tags(Val) when is_binary(Val) ->
    SVal = rabbit_data_coercion:to_list(Val),
    [trim_tag(Tag) || Tag <- re:split(SVal, ",", [{return, list}])];
parse_tags(Val) when is_list(Val) ->
    case hd(Val) of
      Bin when is_binary(Bin) ->
        %% this is a list of binaries
        [trim_tag(Tag) || Tag <- Val];
      Atom when is_atom(Atom) ->
        %% this is a list of atoms
        [trim_tag(Tag) || Tag <- Val];
      Int when is_integer(Int) ->
        %% this is a string/charlist
        [trim_tag(Tag) || Tag <- re:split(Val, ",", [{return, list}])]
    end.

-spec add(vhost:name(), rabbit_types:username()) -> rabbit_types:ok_or_error(any()).

add(VHost, ActingUser) ->
    case exists(VHost) of
        true  -> ok;
        false -> do_add(VHost, <<"">>, [], ActingUser)
    end.

-spec add(vhost:name(), binary(), [atom()], rabbit_types:username()) -> rabbit_types:ok_or_error(any()).

add(Name, Description, Tags, ActingUser) ->
    case exists(Name) of
        true  -> ok;
        false -> do_add(Name, Description, Tags, ActingUser)
    end.

do_add(Name, Description, Tags, ActingUser) ->
    case Description of
        undefined ->
            rabbit_log:info("Adding vhost '~s' without a description", [Name]);
        Value ->
            rabbit_log:info("Adding vhost '~s' (description: '~s', tags: ~p)", [Name, Value, Tags])
    end,
    VHost = rabbit_khepri:try_mnesia_or_khepri(
              fun() -> do_add_to_mnesia(Name, Description, Tags) end,
              fun() -> do_add_to_khepri(Name, Description, Tags) end),
    declare_default_exchanges(Name, ActingUser),
    case rabbit_vhost_sup_sup:start_on_all_nodes(Name) of
        ok ->
            rabbit_event:notify(vhost_created, info(VHost)
                                ++ [{user_who_performed_action, ActingUser},
                                    {description, Description},
                                    {tags, Tags}]),
            ok;
        {error, Reason} ->
            Msg = rabbit_misc:format("failed to set up vhost '~s': ~p",
                                     [Name, Reason]),
            {error, Msg}
    end.

do_add_to_mnesia(Name, Description, Tags) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              case mnesia:wread({rabbit_vhost, Name}) of
                  [] ->
                      Row = vhost:new(Name, [], #{description => Description, tags => Tags}),
                      rabbit_log:debug("Inserting a virtual host record ~p", [Row]),
                      ok = mnesia:write(rabbit_vhost, Row, write),
                      Row;
                  %% the vhost already exists
                  [Row] ->
                      Row
              end
      end).

do_add_to_khepri(Name, Description, Tags) ->
    Path = khepri_vhost_path(Name),
    NewVHost = vhost:new(
                 Name, [], #{description => Description, tags => Tags}),
    case rabbit_khepri:create(Path, NewVHost) of
        {ok, _} ->
            NewVHost;
        {error, {mismatching_node,
                 #{node_path := Path,
                   node_props := #{data := ExistingVHost}}}} ->
            ExistingVHost;
        Error ->
            throw(Error)
    end.

declare_default_exchanges(Name, ActingUser) ->
    DefaultExchanges = [{<<"">>,                   direct,  false},
                        {<<"amq.direct">>,         direct,  false},
                        {<<"amq.topic">>,          topic,   false},
                        %% per 0-9-1 pdf
                        {<<"amq.match">>,          headers, false},
                        %% per 0-9-1 xml
                        {<<"amq.headers">>,        headers, false},
                        {<<"amq.fanout">>,         fanout,  false},
                        {<<"amq.rabbitmq.trace">>, topic,   true}],
    lists:foreach(
      fun({ExchangeName, Type, Internal}) ->
              Resource = rabbit_misc:r(Name, exchange, ExchangeName),
              rabbit_log:debug("Will declare an exchange ~p", [Resource]),
              _ = rabbit_exchange:declare(
                    Resource, Type, true, false, Internal, [], ActingUser)
      end, DefaultExchanges).

-spec update(vhost:name(), binary(), [atom()], rabbit_types:username()) -> rabbit_types:ok_or_error(any()).
update(Name, Description, Tags, ActingUser) ->
    Ret = rabbit_khepri:try_mnesia_or_khepri(
            fun() -> update_in_mnesia(Name, Description, Tags) end,
            fun() -> update_in_khepri(Name, Description, Tags) end),
    case Ret of
        {ok, VHost} ->
            rabbit_event:notify(vhost_updated, info(VHost)
                                ++ [{user_who_performed_action, ActingUser},
                                    {description, Description},
                                    {tags, Tags}]),
            ok;
        _ ->
            Ret
    end.

update_in_mnesia(Name, Description, Tags) ->
    rabbit_misc:execute_mnesia_transaction(
          fun () ->
                  case mnesia:wread({rabbit_vhost, Name}) of
                      [] ->
                          {error, {no_such_vhost, Name}};
                      [VHost0] ->
                          VHost = vhost:merge_metadata(VHost0, #{description => Description, tags => Tags}),
                          rabbit_log:debug("Updating a virtual host record ~p", [VHost]),
                          ok = mnesia:write(rabbit_vhost, VHost, write),
                          {ok, VHost}
                  end
          end).

update_in_khepri(Name, Description, Tags) ->
    Path = khepri_vhost_path(Name),
    Ret1 = rabbit_khepri:get(Path),
    case Ret1 of
        {ok, #{data := VHost0, payload_version := DVersion}} ->
            VHost = vhost:merge_metadata(
                      VHost0, #{description => Description, tags => Tags}),
            rabbit_log:debug("Updating a virtual host record ~p", [VHost]),
            Path1 = khepri_path:combine_with_conditions(
                      Path, [#if_payload_version{version = DVersion}]),
            Ret2 = rabbit_khepri:put(Path1, VHost),
            case Ret2 of
                {ok, _} ->
                    {ok, VHost};
                {error, {mismatching_node, #{node_path := Path}}} ->
                    update_in_khepri(Name, Description, Tags);
                {error, _} = Error ->
                    Error
            end;
        {error, {node_not_found, _}} ->
            {error, {no_such_vhost, Name}};
        {error, _} = Error ->
            Error
    end.

-spec delete(vhost:name(), rabbit_types:username()) -> rabbit_types:ok_or_error(any()).

delete(VHost, ActingUser) ->
    %% FIXME: We are forced to delete the queues and exchanges outside
    %% the TX below. Queue deletion involves sending messages to the queue
    %% process, which in turn results in further mnesia actions and
    %% eventually the termination of that process. Exchange deletion causes
    %% notifications which must be sent outside the TX
    rabbit_log:info("Deleting vhost '~s'", [VHost]),
    %% Clear the permissions first to prohibit new incoming connections when deleting a vhost
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              rabbit_misc:execute_mnesia_transaction(
                with_in_mnesia(
                  VHost,
                  fun() -> clear_permissions_in_mnesia(VHost, ActingUser) end))
      end,
      fun() ->
              ok = clear_permissions_in_khepri(VHost, ActingUser)
      end),
    QDelFun = fun (Q) -> rabbit_amqqueue:delete(Q, false, false, ActingUser) end,
    [begin
         Name = amqqueue:get_name(Q),
         assert_benign(rabbit_amqqueue:with(Name, QDelFun), ActingUser)
     end || Q <- rabbit_amqqueue:list(VHost)],
    [assert_benign(rabbit_exchange:delete(Name, false, ActingUser), ActingUser) ||
        #exchange{name = Name} <- rabbit_exchange:list(VHost)],
    With = with(VHost, fun () -> internal_delete(VHost, ActingUser) end),
    Funs = rabbit_khepri:try_mnesia_or_khepri(
             fun() -> rabbit_misc:execute_mnesia_transaction(With) end,
             %% FIXME: Do we need the atomicity? Currently we can't use a
             %% transaction because of the many side effects here and there in
             %% other modules.
             fun() -> internal_delete(VHost, ActingUser) end),
    ok = rabbit_event:notify(vhost_deleted, [{name, VHost},
                                             {user_who_performed_action, ActingUser}]),
    [case Fun() of
         ok                                  -> ok;
         {error, {no_such_vhost, VHost}} -> ok
     end || Fun <- Funs],
    %% After vhost was deleted from mnesia DB, we try to stop vhost supervisors
    %% on all the nodes.
    rabbit_vhost_sup_sup:delete_on_all_nodes(VHost),
    ok.

put_vhost(Name, Description, Tags0, Trace, Username) ->
    Tags = case Tags0 of
      undefined   -> <<"">>;
      null        -> <<"">>;
      "undefined" -> <<"">>;
      "null"      -> <<"">>;
      Other       -> Other
    end,
    ParsedTags = parse_tags(Tags),
    rabbit_log:debug("Parsed tags ~p to ~p", [Tags, ParsedTags]),
    Result = case exists(Name) of
        true  ->
            update(Name, Description, ParsedTags, Username);
        false ->
            add(Name, Description, ParsedTags, Username),
             %% wait for up to 45 seconds for the vhost to initialise
             %% on all nodes
             case await_running_on_all_nodes(Name, 45000) of
                 ok               ->
                     maybe_grant_full_permissions(Name, Username);
                 {error, timeout} ->
                     {error, timeout}
             end
    end,
    case Trace of
        true      -> rabbit_trace:start(Name);
        false     -> rabbit_trace:stop(Name);
        undefined -> ok
    end,
    Result.

%% when definitions are loaded on boot, Username here will be ?INTERNAL_USER,
%% which does not actually exist
maybe_grant_full_permissions(_Name, ?INTERNAL_USER) ->
    ok;
maybe_grant_full_permissions(Name, Username) ->
    U = rabbit_auth_backend_internal:lookup_user(Username),
    maybe_grant_full_permissions(U, Name, Username).

maybe_grant_full_permissions({ok, _}, Name, Username) ->
    rabbit_auth_backend_internal:set_permissions(
      Username, Name, <<".*">>, <<".*">>, <<".*">>, Username);
maybe_grant_full_permissions(_, _Name, _Username) ->
    ok.


%% 50 ms
-define(AWAIT_SAMPLE_INTERVAL, 50).

-spec await_running_on_all_nodes(vhost:name(), integer()) -> ok | {error, timeout}.
await_running_on_all_nodes(VHost, Timeout) ->
    Attempts = round(Timeout / ?AWAIT_SAMPLE_INTERVAL),
    await_running_on_all_nodes0(VHost, Attempts).

await_running_on_all_nodes0(_VHost, 0) ->
    {error, timeout};
await_running_on_all_nodes0(VHost, Attempts) ->
    case is_running_on_all_nodes(VHost) of
        true  -> ok;
        _     ->
            timer:sleep(?AWAIT_SAMPLE_INTERVAL),
            await_running_on_all_nodes0(VHost, Attempts - 1)
    end.

-spec is_running_on_all_nodes(vhost:name()) -> boolean().
is_running_on_all_nodes(VHost) ->
    States = vhost_cluster_state(VHost),
    lists:all(fun ({_Node, State}) -> State =:= running end,
              States).

-spec vhost_cluster_state(vhost:name()) -> [{atom(), atom()}].
vhost_cluster_state(VHost) ->
    Nodes = rabbit_nodes:all_running(),
    lists:map(fun(Node) ->
        State = case rabbit_misc:rpc_call(Node,
                                          rabbit_vhost_sup_sup, is_vhost_alive,
                                          [VHost]) of
            {badrpc, nodedown} -> nodedown;
            true               -> running;
            false              -> stopped
        end,
        {Node, State}
    end,
    Nodes).

vhost_down(VHost) ->
    ok = rabbit_event:notify(vhost_down,
                             [{name, VHost},
                              {node, node()},
                              {user_who_performed_action, ?INTERNAL_USER}]).

delete_storage(VHost) ->
    VhostDir = msg_store_dir_path(VHost),
    rabbit_log:info("Deleting message store directory for vhost '~s' at '~s'", [VHost, VhostDir]),
    %% Message store should be closed when vhost supervisor is closed.
    case rabbit_file:recursive_delete([VhostDir]) of
        ok                   -> ok;
        {error, {_, enoent}} ->
            %% a concurrent delete did the job for us
            rabbit_log:warning("Tried to delete storage directories for vhost '~s', it failed with an ENOENT", [VHost]),
            ok;
        Other                ->
            rabbit_log:warning("Tried to delete storage directories for vhost '~s': ~p", [VHost, Other]),
            Other
    end.

assert_benign(ok, _)                 -> ok;
assert_benign({ok, _}, _)            -> ok;
assert_benign({ok, _, _}, _)         -> ok;
assert_benign({error, not_found}, _) -> ok;
assert_benign({error, {absent, Q, _}}, ActingUser) ->
    %% Removing the mnesia entries here is safe. If/when the down node
    %% restarts, it will clear out the on-disk storage of the queue.
    QName = amqqueue:get_name(Q),
    rabbit_amqqueue:internal_delete(QName, ActingUser).

internal_delete(VHost, ActingUser) ->
    Fs1 = [rabbit_runtime_parameters:clear(VHost,
                                           proplists:get_value(component, Info),
                                           proplists:get_value(name, Info),
                                           ActingUser)
     || Info <- rabbit_runtime_parameters:list(VHost)],
    Fs2 = [rabbit_policy:delete(VHost, proplists:get_value(name, Info), ActingUser)
           || Info <- rabbit_policy:list(VHost)],
    _ = rabbit_khepri:try_mnesia_or_khepri(
          fun() -> internal_delete_in_mnesia(VHost) end,
          fun() -> internal_delete_in_khepri(VHost) end),
    Fs1 ++ Fs2.

internal_delete_in_mnesia(VHost) ->
    ok = mnesia:delete({rabbit_vhost, VHost}),
    ok.

internal_delete_in_khepri(VHost) ->
    Path = khepri_vhost_path(VHost),
    {ok, Result} = rabbit_khepri:delete(Path),
    %% We reproduce the behavior of `with(...)' here without using it directly
    %% (because it expects to run inside a transaction).
    %%
    %% So if the vhost didn't exist before deletion, we throw an exception.
    case Result =:= #{} of
        false -> ok;
        true  -> throw({error, {no_such_vhost, VHost}})
    end.

-spec exists(vhost:name()) -> boolean().

exists(VHost) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> exists_in_mnesia(VHost) end,
      fun() -> exists_in_khepri(VHost) end).

exists_in_mnesia(VHost) ->
    mnesia:dirty_read({rabbit_vhost, VHost}) /= [].

exists_in_khepri(VHost) ->
    Path = khepri_vhost_path(VHost),
    rabbit_khepri:exists(Path).

-spec list_names() -> [vhost:name()].
list_names() ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> list_names_in_mnesia() end,
      fun() -> list_names_in_khepri() end).

list_names_in_mnesia() ->
    mnesia:dirty_all_keys(rabbit_vhost).

list_names_in_khepri() ->
    Path = khepri_vhosts_path(),
    case rabbit_khepri:list_child_nodes(Path) of
        {ok, Result} -> Result;
        _            -> []
    end.

%% Exists for backwards compatibility, prefer list_names/0.
-spec list() -> [vhost:name()].
list() -> list_names().

-spec all() -> [vhost:vhost()].
all() ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> all_in_mnesia() end,
      fun() -> all_in_khepri() end).

all_in_mnesia() ->
    mnesia:dirty_match_object(rabbit_vhost, vhost:pattern_match_all()).

all_in_khepri() ->
    Path = khepri_vhosts_path(),
    case rabbit_khepri:list_child_data(Path) of
        {ok, VHosts} -> maps:values(VHosts);
        _            -> []
    end.

-spec all_tagged_with(atom()) -> [vhost:vhost()].
all_tagged_with(TagName) ->
    lists:filter(
        fun(VHost) ->
            Meta = vhost:get_metadata(VHost),
            case Meta of
                #{tags := Tags} ->
                    lists:member(rabbit_data_coercion:to_atom(TagName), Tags);
                _ -> false
            end
        end, all()).

-spec count() -> non_neg_integer().
count() ->
    length(list()).

-spec lookup(vhost:name()) -> vhost:vhost() | rabbit_types:ok_or_error(any()).
lookup(VHostName) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> lookup_in_mnesia(VHostName) end,
      fun() -> lookup_in_khepri(VHostName) end).

lookup_in_mnesia(VHostName) ->
    case rabbit_misc:dirty_read({rabbit_vhost, VHostName}) of
        {error, not_found} -> {error, {no_such_vhost, VHostName}};
        {ok, Record}       -> Record
    end.

lookup_in_khepri(VHostName) ->
    Path = khepri_vhost_path(VHostName),
    case rabbit_khepri:get_data(Path) of
        {ok, Record} -> Record;
        _            -> {error, {no_such_vhost, VHostName}}
    end.

-spec with(vhost:name(), rabbit_misc:thunk(A)) -> A.
with(VHostName, Thunk) ->
    fun() ->
            rabbit_khepri:try_mnesia_or_khepri(
              with_in_mnesia(VHostName, Thunk),
              with_in_khepri(VHostName, Thunk))
    end.

with_in_mnesia(VHostName, Thunk) ->
    fun() ->
            case mnesia:read({rabbit_vhost, VHostName}) of
                []   -> mnesia:abort({no_such_vhost, VHostName});
                [_V] -> Thunk()
            end
    end.

with_in_khepri(VHostName, Thunk) ->
    fun() ->
            Path = khepri_vhost_path(VHostName),
            case khepri_tx:exists(Path) of
                true  -> Thunk();
                false -> khepri_tx:abort({no_such_vhost, VHostName})
            end
    end.

-spec with_user_and_vhost(rabbit_types:username(), vhost:name(), rabbit_misc:thunk(A)) -> A.
with_user_and_vhost(Username, VHostName, Thunk) ->
    fun() ->
            rabbit_khepri:try_mnesia_or_khepri(
              rabbit_auth_backend_internal:with_user_in_mnesia(
                Username, with_in_mnesia(VHostName, Thunk)),
              rabbit_auth_backend_internal:with_user_in_khepri(
                Username, with_in_khepri(VHostName, Thunk)))
    end.

with_user_and_vhost_in_mnesia(Username, VHostName, Thunk) ->
    rabbit_auth_backend_internal:with_user_in_mnesia(
      Username, with_in_mnesia(VHostName, Thunk)).

with_user_and_vhost_in_khepri(Username, VHostName, Thunk) ->
    rabbit_auth_backend_internal:with_user_in_khepri(
      Username, with_in_khepri(VHostName, Thunk)).

%% Like with/2 but outside an Mnesia tx

-spec assert(vhost:name()) -> 'ok'.
assert(VHostName) ->
    case exists(VHostName) of
        true  -> ok;
        false -> throw({error, {no_such_vhost, VHostName}})
    end.

-spec update(vhost:name(), fun((vhost:vhost()) -> vhost:vhost())) -> vhost:vhost().
update(VHostName, Fun) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> update_in_mnesia(VHostName, Fun) end,
      fun() -> update_in_khepri(VHostName, Fun) end).

update_in_mnesia(VHostName, Fun) ->
    case mnesia:read({rabbit_vhost, VHostName}) of
        [] ->
            mnesia:abort({no_such_vhost, VHostName});
        [V] ->
            V1 = Fun(V),
            ok = mnesia:write(rabbit_vhost, V1, write),
            V1
    end.

update_in_khepri(VHostName, Fun) ->
    Path = khepri_vhost_path(VHostName),
    case rabbit_khepri:get(Path) of
        {ok, #{data := V, payload_version := DVersion}} ->
            V1 = Fun(V),
            Path1 = khepri_path:combine_with_conditions(
                      Path, [#if_payload_version{version = DVersion}]),
            case rabbit_khepri:put(Path1, V1) of
                {ok, _} ->
                    V1;
                {error, {mismatching_node, _}} ->
                    update(VHostName, Fun);
                Error ->
                    throw(Error)
            end;
        {error, {node_not_found, _}} ->
            throw({error, {no_such_vhost, VHostName}});
        Error ->
            throw(Error)
    end.

-spec update_metadata(vhost:name(), fun((map())-> map())) -> vhost:vhost() | rabbit_types:ok_or_error(any()).
update_metadata(VHostName, Fun) ->
    update(VHostName, fun(Record) ->
        Meta = Fun(vhost:get_metadata(Record)),
        vhost:set_metadata(Record, Meta)
    end).

-spec update_tags(vhost:name(), [vhost_tag()], rabbit_types:username()) -> vhost:vhost() | rabbit_types:ok_or_error(any()).
update_tags(VHostName, Tags, ActingUser) ->
    ConvertedTags = [rabbit_data_coercion:to_atom(I) || I <- Tags],
    Fun = fun() -> update_tags(VHostName, ConvertedTags) end,
    try
        R = rabbit_khepri:try_mnesia_or_khepri(
              fun() -> rabbit_misc:execute_mnesia_transaction(Fun) end,
              fun() -> Fun() end),
        rabbit_log:info("Successfully set tags for virtual host '~s' to ~p", [VHostName, ConvertedTags]),
        rabbit_event:notify(vhost_tags_set, [{name, VHostName},
                                             {tags, ConvertedTags},
                                             {user_who_performed_action, ActingUser}]),
        R
    catch
        throw:{error, {no_such_vhost, _}} = Error ->
            rabbit_log:warning("Failed to set tags for virtual host '~s': the virtual host does not exist", [VHostName]),
            throw(Error);
        throw:Error ->
            rabbit_log:warning("Failed to set tags for virtual host '~s': ~p", [VHostName, Error]),
            throw(Error);
        exit:Error ->
            rabbit_log:warning("Failed to set tags for virtual host '~s': ~p", [VHostName, Error]),
            exit(Error)
    end.

-spec update_tags(vhost:name(), [vhost_tag()]) -> vhost:vhost() | rabbit_types:ok_or_error(any()).
update_tags(VHostName, Tags) ->
    ConvertedTags = [rabbit_data_coercion:to_atom(I) || I <- Tags],
    update(VHostName, fun(Record) ->
        Meta0 = vhost:get_metadata(Record),
        Meta  = maps:update(tags, ConvertedTags, Meta0),
        vhost:set_metadata(Record, Meta)
    end).

-spec tag_with(vhost:name(), [atom()]) -> vhost:vhost() | rabbit_types:ok_or_error(any()).
tag_with(VHostName, Tags) when is_list(Tags) ->
    update_metadata(VHostName, fun(#{tags := Tags0} = Meta) ->
        maps:update(tags, lists:usort(Tags0 ++ Tags), Meta)
    end).

-spec untag_from(vhost:name(), [atom()]) -> vhost:vhost() | rabbit_types:ok_or_error(any()).
untag_from(VHostName, Tags) when is_list(Tags) ->
    update_metadata(VHostName, fun(#{tags := Tags0} = Meta) ->
        maps:update(tags, lists:usort(Tags0 -- Tags), Meta)
    end).

set_limits(VHost, undefined) ->
    vhost:set_limits(VHost, []);
set_limits(VHost, Limits) ->
    vhost:set_limits(VHost, Limits).


dir(Vhost) ->
    <<Num:128>> = erlang:md5(Vhost),
    rabbit_misc:format("~.36B", [Num]).

msg_store_dir_path(VHost) ->
    EncodedName = dir(VHost),
    rabbit_data_coercion:to_list(filename:join([msg_store_dir_base(), EncodedName])).

msg_store_dir_wildcard() ->
    rabbit_data_coercion:to_list(filename:join([msg_store_dir_base(), "*"])).

msg_store_dir_base() ->
    Dir = rabbit_mnesia:dir(),
    filename:join([Dir, "msg_stores", "vhosts"]).

config_file_path(VHost) ->
    VHostDir = msg_store_dir_path(VHost),
    filename:join(VHostDir, ".config").

-spec trim_tag(list() | binary() | atom()) -> atom().
trim_tag(Val) ->
    rabbit_data_coercion:to_atom(string:trim(rabbit_data_coercion:to_list(Val))).

%%----------------------------------------------------------------------------

infos(Items, X) -> [{Item, i(Item, X)} || Item <- Items].

i(name,    VHost) -> vhost:get_name(VHost);
i(tracing, VHost) -> rabbit_trace:enabled(vhost:get_name(VHost));
i(cluster_state, VHost) -> vhost_cluster_state(vhost:get_name(VHost));
i(description, VHost) -> vhost:get_description(VHost);
i(tags, VHost) -> vhost:get_tags(VHost);
i(metadata, VHost) -> vhost:get_metadata(VHost);
i(Item, VHost)     ->
  rabbit_log:error("Don't know how to compute a virtual host info item '~s' for virtual host '~p'", [Item, VHost]),
  throw({bad_argument, Item}).

-spec info(vhost:vhost() | vhost:name()) -> rabbit_types:infos().

info(VHost) when ?is_vhost(VHost) ->
    infos(?INFO_KEYS, VHost);
info(Key) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> info_in_mnesia(Key) end,
      fun() -> info_in_khepri(Key) end).

info_in_mnesia(Key) ->
    case mnesia:dirty_read({rabbit_vhost, Key}) of
        [] -> [];
        [VHost] -> infos(?INFO_KEYS, VHost)
    end.

info_in_khepri(Key) ->
    Path = khepri_vhost_path(Key),
    case rabbit_khepri:get_data(Path) of
        {ok, VHost} -> infos(?INFO_KEYS, VHost);
        _           -> []
    end.

-spec info(vhost:vhost(), rabbit_types:info_keys()) -> rabbit_types:infos().
info(VHost, Items) -> infos(Items, VHost).

-spec info_all() -> [rabbit_types:infos()].
info_all()       -> info_all(?INFO_KEYS).

-spec info_all(rabbit_types:info_keys()) -> [rabbit_types:infos()].
info_all(Items) -> [info(VHost, Items) || VHost <- all()].

info_all(Ref, AggregatorPid)        -> info_all(?INFO_KEYS, Ref, AggregatorPid).

-spec info_all(rabbit_types:info_keys(), reference(), pid()) -> 'ok'.
info_all(Items, Ref, AggregatorPid) ->
    rabbit_control_misc:emitting_map(
       AggregatorPid, Ref, fun(VHost) -> info(VHost, Items) end, all()).

clear_permissions_in_mnesia(VHost, ActingUser) ->
    [ok = rabbit_auth_backend_internal:clear_permissions(
            proplists:get_value(user, Info), VHost, ActingUser)
     || Info <-
        rabbit_auth_backend_internal:list_vhost_permissions(VHost)],
    TopicPermissions =
    rabbit_auth_backend_internal:list_vhost_topic_permissions(VHost),
    [ok = rabbit_auth_backend_internal:clear_topic_permissions(
        proplists:get_value(user, TopicPermission), VHost, ActingUser)
     || TopicPermission <- TopicPermissions].

clear_permissions_in_khepri(VHost, ActingUser) ->
    ok = rabbit_auth_backend_internal:clear_vhost_permissions_in_khepri(
           VHost, ActingUser),
    ok = rabbit_auth_backend_internal:clear_vhost_topic_permissions_in_khepri(
           VHost, ActingUser).

clear_data_in_khepri() ->
    Path = khepri_vhosts_path(),
    case rabbit_khepri:delete(Path) of
        {ok, _} -> ok;
        Error -> throw(Error)
    end.

mnesia_write_to_khepri(VHost) when ?is_vhost(VHost) ->
    Name = vhost:get_name(VHost),
    Path = khepri_vhost_path(Name),
    case rabbit_khepri:put(Path, VHost) of
        {ok, _} -> ok;
        Error -> throw(Error)
    end.

mnesia_delete_to_khepri(VHost) when ?is_vhost(VHost) ->
    Name = vhost:get_name(VHost),
    Path = khepri_vhost_path(Name),
    case rabbit_khepri:delete(Path) of
        {ok, _} -> ok;
        Error -> throw(Error)
    end.

khepri_vhosts_path()     -> [?MODULE].
khepri_vhost_path(VHost) -> [?MODULE, VHost].
