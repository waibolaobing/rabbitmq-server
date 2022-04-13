%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2018-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(listeners_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile([nowarn_export_all, export_all]).
-compile(export_all).

suite() ->
    [{timetrap, 5 * 60000}].

all() ->
    [
     {group, mnesia_store},
     {group, khepri_store},
     {group, khepri_migration}
    ].

groups() ->
    [
     {mnesia_store, [], all_tests()},
     {khepri_store, [], all_tests()},
     {khepri_migration, [], [
                             from_mnesia_to_khepri
                            ]}
    ].

all_tests() ->
    [
     connections,
     listener_of_protocol,
     node_listeners
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config, []).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(mnesia_store = Group, Config) ->
    init_per_group_common(Group, Config, 1);
init_per_group(khepri_store = Group, Config0) ->
    Config = init_per_group_common(Group, Config0, 1),
    enable_khepri(Group, Config);
init_per_group(khepri_migration = Group, Config) ->
    init_per_group_common(Group, Config, 1).

enable_khepri(Group, Config) ->
    case rabbit_ct_broker_helpers:enable_feature_flag(Config, raft_based_metadata_store_phase1) of
        ok ->
            Config;
        Skip ->
            end_per_group(Group, Config),
            Skip
    end.

init_per_group_common(Group, Config, Size) ->
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodes_count, Size},
                                            {rmq_nodename_suffix, Group},
                                            {tcp_ports_base}]),
    rabbit_ct_helpers:run_steps(Config1, rabbit_ct_broker_helpers:setup_steps()).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:testcase_started(Config, Testcase),
    rabbit_ct_helpers:run_steps(Config1, rabbit_ct_client_helpers:setup_steps()).

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(Config, rabbit_ct_client_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------
connections(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    
    Conn = rabbit_ct_client_helpers:open_connection(Config, Server),
    ?assertMatch([_], rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_networking, connections, [])),
    rabbit_ct_client_helpers:close_connection(Conn),
    ?assertMatch([], rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_networking, connections, [])),

    ok.

listener_of_protocol(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    ?assertMatch(
       #listener{protocol = amqp,
                 node = Server},
       rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_networking, listener_of_protocol, [amqp])),
    ?assertMatch(
       #listener{protocol = clustering,
                 node = Server},
       rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_networking, listener_of_protocol, [clustering])).

node_listeners(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Listeners =
        rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_networking, node_listeners, [Server]),
    ?assertMatch([#listener{protocol = amqp, node = Server}],
                 [L || L <- Listeners, L#listener.protocol == amqp]),
    ?assertMatch([#listener{protocol = clustering, node = Server}],
                 [L || L <- Listeners, L#listener.protocol == clustering]).    
    
from_mnesia_to_khepri(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    ?assertMatch(
       #listener{protocol = amqp,
                 node = Server},
       rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_networking, listener_of_protocol, [amqp])),
    
    case rabbit_ct_broker_helpers:enable_feature_flag(Config, raft_based_metadata_store_phase1) of
        ok ->
            ?assertMatch(
               #listener{protocol = amqp,
                         node = Server},
               rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_networking, listener_of_protocol, [amqp]));
        Skip ->
            Skip
    end.
