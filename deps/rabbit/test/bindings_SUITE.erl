%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2018-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(bindings_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile([nowarn_export_all, export_all]).
-compile(export_all).

suite() ->
    [{timetrap, 5 * 60000}].

all() ->
    [
      {group, single_node}
    ].

groups() ->
    [
     {single_node, [], all_tests()}
    ].

all_tests() ->
    [
     bind_and_unbind,
     bind_and_delete,
     list_bindings,
     list_for_source,
     list_for_destination
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config, []).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodes_count, 1},
                                            {rmq_nodename_suffix, Group},
                                            {tcp_ports_base}]),
    Ret = rabbit_ct_helpers:run_steps(Config1, rabbit_ct_broker_helpers:setup_steps()),
    case Ret of
        {skip, _} ->
            Ret;
        Config2 ->
            EnableFF = rabbit_ct_broker_helpers:enable_feature_flag(
                         Config2, raft_based_metadata_store_phase1),
            case EnableFF of
                ok ->
                    Config2;
                Skip ->
                    end_per_group(Group, Config2),
                    Skip
            end
    end.

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:testcase_started(Config, Testcase),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queues, []),
    Q = rabbit_data_coercion:to_binary(Testcase),
    Config2 = rabbit_ct_helpers:set_config(Config1,
                                           [{queue_name, Q},
                                            {alt_queue_name, <<Q/binary, "_alt">>}
                                           ]),
    rabbit_ct_helpers:run_steps(Config2, rabbit_ct_client_helpers:setup_steps()).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queues, []),
    Config1 = rabbit_ct_helpers:run_steps(
                Config,
                rabbit_ct_client_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% TODO run for both mnesia and khepri!

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------
bind_and_unbind(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q, [])),

    DefaultExchange = rabbit_misc:r(<<"/">>, exchange, <<>>),
    QResource = rabbit_misc:r(<<"/">>, queue, Q),
    DefaultBinding = #binding{source = DefaultExchange,
                              destination = QResource,
                              key = Q,
                              args = []},
    
    %% Binding to the default exchange, it's always present
    ?assertEqual([DefaultBinding],
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>])),
    
    %% Let's bind to other exchange
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.direct">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    
    DirectBinding = #binding{source = rabbit_misc:r(<<"/">>, exchange, <<"amq.direct">>),
                             destination = QResource,
                             key = Q,
                             args = []},
    Bindings = lists:sort([DefaultBinding, DirectBinding]),
    
    ?assertEqual(Bindings,
                 lists:sort(
                   rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>]))),
    
    #'queue.unbind_ok'{} = amqp_channel:call(Ch, #'queue.unbind'{exchange = <<"amq.direct">>,
                                                                 queue = Q,
                                                                 routing_key = Q}),
    
    ?assertEqual([DefaultBinding],
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>])),
    ok.

bind_and_delete(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q, [])),

    DefaultExchange = rabbit_misc:r(<<"/">>, exchange, <<>>),
    QResource = rabbit_misc:r(<<"/">>, queue, Q),
    DefaultBinding = #binding{source = DefaultExchange,
                              destination = QResource,
                              key = Q,
                              args = []},
    
    %% Binding to the default exchange, it's always present
    ?assertEqual([DefaultBinding],
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>])),
    
    %% Let's bind to other exchange
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.direct">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    
    DirectBinding = #binding{source = rabbit_misc:r(<<"/">>, exchange, <<"amq.direct">>),
                             destination = QResource,
                             key = Q,
                             args = []},
    Bindings = lists:sort([DefaultBinding, DirectBinding]),
    
    ?assertEqual(Bindings,
                 lists:sort(
                   rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>]))),
    
    ?assertMatch(#'queue.delete_ok'{},
                 amqp_channel:call(Ch, #'queue.delete'{queue = Q})),
    
    ?assertEqual([],
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>])),
    ok.

list_bindings(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q, [])),

    DefaultExchange = rabbit_misc:r(<<"/">>, exchange, <<>>),
    QResource = rabbit_misc:r(<<"/">>, queue, Q),
    DefaultBinding = #binding{source = DefaultExchange,
                              destination = QResource,
                              key = Q,
                              args = []},
    
    %% Binding to the default exchange, it's always present
    ?assertEqual([DefaultBinding],
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>])),
    
    %% Let's bind to all other exchanges
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.direct">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.fanout">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.headers">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.match">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.rabbitmq.trace">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.topic">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    
    DirectBinding = #binding{source = rabbit_misc:r(<<"/">>, exchange, <<"amq.direct">>),
                             destination = QResource,
                             key = Q,
                             args = []},
    FanoutBinding = #binding{source = rabbit_misc:r(<<"/">>, exchange, <<"amq.fanout">>),
                             destination = QResource,
                             key = Q,
                             args = []},
    HeadersBinding = #binding{source = rabbit_misc:r(<<"/">>, exchange, <<"amq.headers">>),
                              destination = QResource,
                              key = Q,
                              args = []},
    MatchBinding = #binding{source = rabbit_misc:r(<<"/">>, exchange, <<"amq.match">>),
                            destination = QResource,
                            key = Q,
                            args = []},
    TraceBinding = #binding{source = rabbit_misc:r(<<"/">>, exchange, <<"amq.rabbitmq.trace">>),
                            destination = QResource,
                            key = Q,
                            args = []},
    TopicBinding = #binding{source = rabbit_misc:r(<<"/">>, exchange, <<"amq.topic">>),
                            destination = QResource,
                            key = Q,
                            args = []},
    Bindings = lists:sort([DefaultBinding, DirectBinding, FanoutBinding, HeadersBinding,
                           MatchBinding, TraceBinding, TopicBinding]),
    
    ?assertEqual(Bindings,
                 lists:sort(
                   rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>]))),

    ok.

list_for_source(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    QAlt = ?config(alt_queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q, [])),
    ?assertEqual({'queue.declare_ok', QAlt, 0, 0}, declare(Ch, QAlt, [])),
    
    QResource = rabbit_misc:r(<<"/">>, queue, Q),
    QAltResource = rabbit_misc:r(<<"/">>, queue, QAlt),
    
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.direct">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.topic">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.direct">>,
                                                             queue = QAlt,
                                                             routing_key = QAlt}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.topic">>,
                                                             queue = QAlt,
                                                             routing_key = QAlt}),

    DirectExchange = rabbit_misc:r(<<"/">>, exchange, <<"amq.direct">>),
    TopicExchange = rabbit_misc:r(<<"/">>, exchange, <<"amq.topic">>),
    DirectBinding = #binding{source = DirectExchange,
                             destination = QResource,
                             key = Q,
                             args = []},
    DirectABinding = #binding{source = DirectExchange,
                              destination = QAltResource,
                              key = QAlt,
                              args = []},
    TopicBinding = #binding{source = TopicExchange,
                            destination = QResource,
                            key = Q,
                            args = []},
    TopicABinding = #binding{source = TopicExchange,
                             destination = QAltResource,
                             key = QAlt,
                             args = []},
    DirectBindings = lists:sort([DirectBinding, DirectABinding]),
    TopicBindings = lists:sort([TopicBinding, TopicABinding]),
    
    ?assertEqual(
       DirectBindings,
       lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list_for_source,
                                               [DirectExchange]))),
    ?assertEqual(
       TopicBindings,
       lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list_for_source,
                                               [TopicExchange]))).    

list_for_destination(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    QAlt = ?config(alt_queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q, [])),
    ?assertEqual({'queue.declare_ok', QAlt, 0, 0}, declare(Ch, QAlt, [])),
    
    QResource = rabbit_misc:r(<<"/">>, queue, Q),
    QAltResource = rabbit_misc:r(<<"/">>, queue, QAlt),
    
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.direct">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.topic">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.direct">>,
                                                             queue = QAlt,
                                                             routing_key = QAlt}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.topic">>,
                                                             queue = QAlt,
                                                             routing_key = QAlt}),

    DirectExchange = rabbit_misc:r(<<"/">>, exchange, <<"amq.direct">>),
    TopicExchange = rabbit_misc:r(<<"/">>, exchange, <<"amq.topic">>),
    DefaultExchange = rabbit_misc:r(<<"/">>, exchange, <<>>),
    DirectBinding = #binding{source = DirectExchange,
                             destination = QResource,
                             key = Q,
                             args = []},
    DirectABinding = #binding{source = DirectExchange,
                              destination = QAltResource,
                              key = QAlt,
                              args = []},
    TopicBinding = #binding{source = TopicExchange,
                            destination = QResource,
                            key = Q,
                            args = []},
    TopicABinding = #binding{source = TopicExchange,
                             destination = QAltResource,
                             key = QAlt,
                             args = []},
    DefaultBinding = #binding{source = DefaultExchange,
                              destination = QResource,
                              key = Q,
                              args = []},
    DefaultABinding = #binding{source = DefaultExchange,
                               destination = QAltResource,
                               key = QAlt,
                               args = []},

    Bindings = lists:sort([DefaultBinding, DirectBinding, TopicBinding]),
    AltBindings = lists:sort([DefaultABinding, DirectABinding, TopicABinding]),
    
    ?assertEqual(
       Bindings,
       lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list_for_destination,
                                               [QResource]))),
    ?assertEqual(
       AltBindings,
       lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list_for_destination,
                                               [QAltResource]))).

delete_queues() ->
    [{ok, _} = rabbit_amqqueue:delete(Q, false, false, <<"dummy">>)
     || Q <- rabbit_amqqueue:list()].

declare(Ch, Q, Args) ->
    amqp_channel:call(Ch, #'queue.declare'{queue     = Q,
                                           durable   = true,
                                           auto_delete = false,
                                           arguments = Args}).
