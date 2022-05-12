%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_store).

-include_lib("khepri/include/khepri.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include("amqqueue.hrl").

-export([list_exchanges/0, count_exchanges/0, list_exchange_names/0,
         update_exchange_decorators/1, update_exchange_scratch/2,
         create_exchange/2, list_exchanges/1, list_durable_exchanges/0,
         lookup_exchange/1, lookup_many_exchanges/1, peek_exchange_serial/3,
         next_exchange_serial/1, delete_exchange_in_khepri/3,
         delete_exchange_in_mnesia/3, delete_exchange/3,
         recover_exchanges/1, store_durable_exchanges/1]).

-export([mnesia_write_exchange_to_khepri/1, mnesia_write_durable_exchange_to_khepri/1,
         mnesia_write_exchange_serial_to_khepri/1,
         clear_exchange_data_in_khepri/0, clear_durable_exchange_data_in_khepri/0,
         clear_exchange_serial_data_in_khepri/0]).

-export([exists_binding/1, add_binding/2, delete_binding/2, list_bindings/1,
         list_bindings_for_source/1, list_bindings_for_destination/1,
         list_bindings_for_source_and_destination/2, list_explicit_bindings/0,
         recover_bindings/0, recover_bindings/1, delete_binding/1]).

%% TODO used by rabbit_policy, to become internal
-export([update_exchange_in_mnesia/2, update_exchange_in_khepri/2]).

%% TODO used by rabbit policy. to become internal
-export([list_exchanges_in_mnesia/1, list_queues_in_khepri_tx/1,
         list_queues_in_mnesia/1, update_queue_in_mnesia/2, update_queue_in_khepri/2]).
%% TODO used by topic exchange. Should it be internal?
-export([match_source_and_destination_in_khepri/2]).

-export([mnesia_write_route_to_khepri/1, mnesia_write_durable_route_to_khepri/1,
         mnesia_write_semi_durable_route_to_khepri/1, mnesia_write_reverse_route_to_khepri/1,
         clear_route_in_khepri/0]).

-export([list_queues/0, list_queues/1, list_durable_queues/0, list_durable_queues/1,
         list_durable_queues_by_type/1, list_queue_names/0, count_queues/0, count_queues/1,
         delete_queue/2, internal_delete_queue/3, update_queue/2, lookup_queues/1,
         lookup_queue/1, lookup_durable_queue/1, delete_transient_queues/1,
         update_queue_decorators/1, not_found_or_absent_queue_dirty/1,
         lookup_durable_queues/1]).

-export([mnesia_write_queue_to_khepri/1, mnesia_write_durable_queue_to_khepri/1,
         mnesia_delete_queue_to_khepri/1, mnesia_delete_durable_queue_to_khepri/1,
         clear_queue_data_in_khepri/0, clear_durable_queue_data_in_khepri/0,
         store_queue/2, store_queues/1, store_queue_without_recover/2,
         store_queue_dirty/1]).

%% Routing. These functions are in the hot code path
-export([match_bindings/2, match_routing_key/2]).

-export([add_listener/1, list_listeners/1, list_listeners/2,
         delete_listener/1, delete_listeners/1]).

-export([mnesia_write_listener_to_khepri/1, clear_listener_data_in_khepri/0]).

%% TODO maybe refactor after queues are migrated.
-export([store_durable_queue/1]).

%% TODO to become internal
-export([list_exchanges_in_khepri_tx/1,
         lookup_queue_in_khepri_tx/1]).

%% Paths
%% --------------------------------------------------------------

%% Exchanges
khepri_exchanges_path() ->
    [?MODULE, exchanges].

khepri_exchange_path(#resource{virtual_host = VHost, name = Name}) ->
    [?MODULE, exchanges, VHost, Name].

khepri_exchange_serials_path() ->
    [?MODULE, exchanges_serials].

khepri_exchange_serial_path(#resource{virtual_host = VHost, name = Name}) ->
    [?MODULE, exchange_serials, VHost, Name].

%% Bindings

khepri_route_path(#binding{source = #resource{virtual_host = VHost, name = SrcName},
                           destination = #resource{kind = Kind, name = DstName},
                           key = RoutingKey}) ->
    [?MODULE, routes, VHost, SrcName, Kind, DstName, RoutingKey].

khepri_routes_path() ->
    [?MODULE, routes].

%% Routing optimisation, probably the most relevant on the hot code path.
%% It only needs to store a list of destinations to be used by rabbit_router.
%% Unless there is a high queue churn, this should barely change. Thus, the small
%% penalty for updates should be worth it.
khepri_routing_path() ->
    [?MODULE, routing].

khepri_routing_path(#binding{source = Src, key = RoutingKey}) ->
    khepri_routing_path(Src, RoutingKey).

khepri_routing_path(#resource{virtual_host = VHost, name = Name}, RoutingKey) ->
    [?MODULE, routing, VHost, Name, RoutingKey].

%% Queues
khepri_queues_path() ->
    [?MODULE, queues].

khepri_queue_path(#resource{virtual_host = VHost, name = Name}) ->
    [?MODULE, queues, VHost, Name].

khepri_durable_queues_path() ->
    [?MODULE, durable_queues].

khepri_durable_queue_path(#resource{virtual_host = VHost, name = Name}) ->
    [?MODULE, durable_queues, VHost, Name].

%% Listeners
khepri_listener_path(Node) ->
    [?MODULE, listeners, Node].

khepri_listeners_path() ->
    [?MODULE, listeners].

%% API
%% --------------------------------------------------------------
create_exchange(#exchange{name = XName} = X, PrePostCommitFun) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              execute_transaction(
                fun() -> create_exchange_in_mnesia({rabbit_exchange, XName}, X) end,
                PrePostCommitFun,
                mnesia)
      end,
      fun() ->
              execute_transaction(
                fun() -> create_exchange_in_khepri(khepri_exchange_path(XName), X) end,
                PrePostCommitFun,
                khepri)
      end).

list_exchanges() ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> list_in_mnesia(rabbit_exchange, #exchange{_ = '_'}) end,
      fun() -> list_in_khepri(khepri_exchanges_path() ++ [?STAR_STAR]) end).

count_exchanges() ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> count_in_mnesia(rabbit_exchange) end,
      fun() -> count_in_khepri(khepri_exchanges_path()) end).

list_exchange_names() ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> list_names_in_mnesia(rabbit_exchange) end,
      fun() -> list_names_in_khepri(khepri_exchanges_path()) end).

list_exchanges(VHost) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              list_exchanges_in_mnesia(VHost)
      end,
      fun() ->
              list_in_khepri(khepri_exchanges_path() ++ [VHost, ?STAR_STAR])
      end).

%% TODO should be internal once rabbit_policy is migrated
list_exchanges_in_mnesia(VHost) ->
    Match = #exchange{name = rabbit_misc:r(VHost, exchange), _ = '_'},
    list_in_mnesia(rabbit_exchange, Match).

list_durable_exchanges() ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              list_in_mnesia(rabbit_durable_exchange, #exchange{_ = '_'})
      end,
      fun() ->
              Pattern = #if_data_matches{pattern = #exchange{durable = true, _ = '_'}},
              list_in_khepri(khepri_exchanges_path() ++ [?STAR, Pattern])
      end).

lookup_exchange(Name) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> lookup({rabbit_exchange, Name}, mnesia) end,
      fun() -> lookup(khepri_exchange_path(Name), khepri) end).

lookup_many_exchanges(Names) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> lookup_many(rabbit_exchange, Names, mnesia) end,
      fun() -> lookup_many(fun khepri_exchange_path/1, Names, khepri) end).

%% TODO rabbit_policy:update_matched_objects_in_khepri
list_exchanges_in_khepri_tx(VHostPath) ->
    Path = khepri_exchanges_path() ++ [VHostPath, ?STAR_STAR],
    {ok, Map} = rabbit_khepri:tx_match_and_get_data(Path),
    maps:values(Map).

peek_exchange_serial(XName, LockType, mnesia) ->
    case mnesia:read(rabbit_exchange_serial, XName, LockType) of
        [#exchange_serial{next = Serial}]  -> Serial;
        _                                  -> 1
    end;
peek_exchange_serial(XName, _LockType, khepri) ->
    Path = khepri_exchange_serial_path(XName),
    case khepri_tx:get(Path) of
        {ok, #{Path := #{data := #exchange_serial{next = Serial}}}} ->
            Serial;
        _ ->
            1
    end.

next_exchange_serial(X) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              rabbit_misc:execute_mnesia_transaction(fun() ->
                                                             next_exchange_serial_in_mnesia(X)
                                                     end)
      end,
      fun() ->
              rabbit_khepri:transaction(fun() ->
                                                next_exchange_serial_in_khepri(X)
                                        end, rw)
      end).

next_exchange_serial_in_mnesia(#exchange{name = XName}) ->
    Serial = peek_exchange_serial(XName, write, mnesia),
    ok = mnesia:write(rabbit_exchange_serial,
                      #exchange_serial{name = XName, next = Serial + 1}, write),
    Serial.

next_exchange_serial_in_khepri(#exchange{name = XName}) ->
    Path = khepri_exchange_serial_path(XName),
    Extra = #{keep_while => #{khepri_exchange_path(XName) => #if_node_exists{exists = true}}},
    Serial = case khepri_tx:get(Path) of
                 {ok, #{Path := #{data := #exchange_serial{next = Serial0}}}} -> Serial0;
                 _ -> 1
             end,
    {ok, _} = khepri_tx:put(Path,
                            #exchange_serial{name = XName, next = Serial + 1},
                            Extra),
    Serial.

update_exchange_in_mnesia(Name, Fun) ->
    Table = {rabbit_exchange, Name},
    case lookup_tx_in_mnesia(Table) of
        [X] -> X1 = Fun(X),
               store_exchange_in_mnesia(X1);
        [] -> not_found
    end.

update_exchange_in_khepri(Name, Fun) ->
    Path = khepri_exchange_path(Name),
    case lookup_tx_in_khepri(Path) of
        [X] -> X1 = Fun(X),
               store_exchange_in_khepri(X1);
        [] -> not_found
    end.

update_exchange_decorators(Name) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> update_exchange_decorators(Name, mnesia) end,
      fun() -> update_exchange_decorators(Name, khepri) end).

update_exchange_scratch(Name, ScratchFun) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              rabbit_misc:execute_mnesia_transaction(
                fun() ->
                        update_exchange_in_mnesia(Name, ScratchFun)
                end)
      end,
      fun() ->
              rabbit_khepri:transaction(
                fun() ->
                        update_exchange_in_khepri(Name, ScratchFun)
                end, rw)
      end).

store_durable_exchanges(Xs) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              rabbit_misc:execute_mnesia_transaction(
                fun () ->
                        [mnesia:write(rabbit_durable_exchange, X, write) || X <- Xs]
                end)
      end,
      fun() ->
              rabbit_khepri:transaction(
                fun() ->
                        [store_exchange_in_khepri(X) || X <- Xs]
                end, rw)
      end).

delete_exchange_in_mnesia(X = #exchange{name = XName}, OnlyDurable, RemoveBindingsForSource) ->
    ok = mnesia:delete({rabbit_exchange, XName}),
    mnesia:delete({rabbit_durable_exchange, XName}),
    remove_bindings_in_mnesia(X, OnlyDurable, RemoveBindingsForSource).

delete_exchange_in_khepri(X = #exchange{name = XName}, OnlyDurable, RemoveBindingsForSource) ->
    {ok, _} = khepri_tx:delete(khepri_exchange_path(XName)),
    remove_bindings_in_khepri(X, OnlyDurable, RemoveBindingsForSource).

delete_exchange(XName, IfUnused, PrePostCommitFun) ->
    rabbit_khepri:try_mnesia_or_khepri(
      delete_exchange_fun({rabbit_exchange, XName}, IfUnused, PrePostCommitFun, mnesia),
      delete_exchange_fun(khepri_exchange_path(XName), IfUnused, PrePostCommitFun, khepri)).

recover_exchanges(VHost) ->
   rabbit_khepri:try_mnesia_or_khepri(
     fun() -> recover_exchanges(VHost, mnesia) end,
     fun() -> recover_exchanges(VHost, khepri) end).

%% Bindings

exists_binding(#binding{source = SrcName,
                        destination = DstName} = Binding) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              binding_action_in_mnesia(
                Binding, fun (_Src, _Dst) ->
                                 rabbit_misc:const(mnesia:read({rabbit_route, Binding}) /= [])
                         end, fun not_found_or_absent_errs_in_mnesia/1)
      end,
      fun() ->
              Path = khepri_route_path(Binding),
              rabbit_khepri:transaction(
                fun () ->
                        case {lookup_resource_in_khepri_tx(SrcName),
                              lookup_resource_in_khepri_tx(DstName)} of
                            {[_Src], [_Dst]} ->
                                exists_binding_in_khepri(Path, Binding);
                            Errs -> not_found_or_absent_errs_in_khepri(
                                      not_found(Errs, SrcName, DstName))
                        end
                end, ro)
      end).

add_binding(Binding, ChecksFun) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> add_binding_in_mnesia(Binding, ChecksFun) end,
      fun() -> add_binding_in_khepri(Binding, ChecksFun) end).

delete_binding(Binding, ChecksFun) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> remove_binding_in_mnesia(Binding, ChecksFun) end,
      fun() -> remove_binding_in_khepri(Binding, ChecksFun) end).

list_bindings(VHost) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              VHostResource = rabbit_misc:r(VHost, '_'),
              Match = #route{binding = #binding{source      = VHostResource,
                                                destination = VHostResource,
                                                _           = '_'},
                             _       = '_'},
              [B || #route{binding = B} <- list_in_mnesia(rabbit_route, Match)]
      end,
      fun() ->
              Path = khepri_routes_path() ++ [VHost, ?STAR_STAR],
              lists:foldl(fun(#{bindings := SetOfBindings}, Acc) ->
                                  sets:to_list(SetOfBindings) ++ Acc
                          end, [], list_in_khepri(Path))
      end).

list_bindings_for_source(#resource{virtual_host = VHost, name = Name} = Resource) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              Route = #route{binding = #binding{source = Resource, _ = '_'}},
              [B || #route{binding = B} <- list_in_mnesia(rabbit_route, Route)]
      end,
      fun() ->
              Path = khepri_routes_path() ++ [VHost, Name, ?STAR_STAR],
              lists:foldl(fun(#{bindings := SetOfBindings}, Acc) ->
                                  sets:to_list(SetOfBindings) ++ Acc
                          end, [], list_in_khepri(Path))
      end).

list_bindings_for_destination(#resource{virtual_host = VHost, name = Name,
                                        kind = Kind} = Resource) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              Route = rabbit_binding:reverse_route(#route{binding = #binding{destination = Resource,
                                                                             _ = '_'}}),
              [rabbit_binding:reverse_binding(B) ||
                  #reverse_route{reverse_binding = B} <- list_in_mnesia(rabbit_reverse_route, Route)]
      end,
      fun() ->
              Path = khepri_routes_path() ++ [VHost, ?STAR, Kind, Name, ?STAR_STAR],
              lists:foldl(fun(#{bindings := SetOfBindings}, Acc) ->
                                  sets:to_list(SetOfBindings) ++ Acc
                          end, [], list_in_khepri(Path))
      end).

list_bindings_for_source_and_destination(SrcName, DstName) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              Route = #route{binding = #binding{source      = SrcName,
                                                destination = DstName,
                                                _           = '_'}},
              [B || #route{binding = B} <- list_in_mnesia(rabbit_route, Route)]
      end,
      fun() ->
              Values = match_source_and_destination_in_khepri(SrcName, DstName),
              lists:foldl(fun(#{bindings := SetOfBindings}, Acc) ->
                                  sets:to_list(SetOfBindings) ++ Acc
                          end, [], Values)
      end).

list_explicit_bindings() ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              mnesia:async_dirty(
                fun () ->
                        AllRoutes = mnesia:dirty_match_object(rabbit_route, #route{_ = '_'}),
                        %% if there are any default exchange bindings left after an upgrade
                        %% of a pre-3.8 database, filter them out
                        AllBindings = [B || #route{binding = B} <- AllRoutes],
                        lists:filter(fun(#binding{source = S}) ->
                                             not (S#resource.kind =:= exchange andalso S#resource.name =:= <<>>)
                                     end, AllBindings)
                end)
      end,
      fun() ->
              Condition = #if_not{condition = #if_name_matches{regex = "^$"}},
              Path = khepri_routes_path() ++ [?STAR, Condition, ?STAR_STAR],
              {ok, Data} = rabbit_khepri:match_and_get_data(Path),
              lists:foldl(fun(#{bindings := SetOfBindings}, Acc) ->
                                  sets:to_list(SetOfBindings) ++ Acc
                          end, [], maps:values(Data))
      end).

recover_bindings() ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> recover_bindings_in_mnesia() end,
      %% Nothing to do in khepri, single table storage
      fun() -> ok end).

recover_bindings(RecoverFun) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              [RecoverFun(Route, Src, Dst, fun recover_semi_durable_route_txn/3, mnesia) ||
                  #route{binding = #binding{destination = Dst,
                                            source = Src}} = Route <-
                      rabbit_misc:dirty_read_all(rabbit_semi_durable_route)]
      end,
      fun() ->
              Root = khepri_routes_path(),
              {ok, Map} = rabbit_khepri:match_and_get_data(
                            Root ++ [?STAR_STAR, #if_data_matches{pattern = #{type => semi_durable}}]),
              maps:foreach(
                fun(Path, _) ->
                        Dst = destination_from_khepri_path(Path),
                        Src = source_from_khepri_path(Path),
                        RecoverFun(Path, Src, Dst, fun recover_semi_durable_route_txn/3, khepri)
                end, Map)
      end).

%% Implicit bindings are implicit as of rabbitmq/rabbitmq-server#1721.
delete_binding(Binding) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              delete_binding_in_mnesia(Binding)
      end,
      fun() ->
              delete_binding_in_khepri(Binding)
      end).

%% Queues
list_queues() ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> list_queues_with_possible_retry_in_mnesia(
                 fun() ->
                         list_in_mnesia(rabbit_queue, amqqueue:pattern_match_all())
                 end)
      end,
      fun() -> list_queues_with_possible_retry_in_khepri(
                 fun() ->
                         list_in_khepri(khepri_queues_path() ++ [?STAR_STAR])
                 end)
      end).

list_durable_queues() ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> list_queues_with_possible_retry_in_mnesia(
                 fun() ->
                         list_in_mnesia(rabbit_durable_queue, amqqueue:pattern_match_all())
                 end)
      end,
      fun() -> list_queues_with_possible_retry_in_khepri(
                 fun() ->
                         list_in_khepri(khepri_durable_queues_path() ++ [?STAR_STAR])
                 end)
      end).

list_durable_queues_by_type(Type) ->
    Pattern = amqqueue:pattern_match_on_type(Type),
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              list_in_mnesia(rabbit_durable_queue, Pattern)
      end,
      fun() ->
              list_in_khepri(khepri_queues_path() ++ [#if_data_matches{pattern = Pattern}])
      end).

list_queues(VHost) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              list_queues_in_mnesia(VHost)
      end,
      fun() -> list_queues_with_possible_retry_in_khepri(
                 fun() ->
                         list_in_khepri(khepri_queues_path() ++ [VHost, ?STAR_STAR])
                 end)
      end).

%% TODO to become internal, used by rabbit_policy
list_queues_in_mnesia(VHost) ->
    list_queues_with_possible_retry_in_mnesia(
      fun() ->
              Pattern = amqqueue:pattern_match_on_name(rabbit_misc:r(VHost, queue)),
              list_in_mnesia(rabbit_queue, Pattern)
      end).

%% TODO to become internal, used by rabbit_policy
list_queues_in_khepri_tx(VHostPath) ->
    Path = khepri_queues_path(),
    {ok, Map} = rabbit_khepri:tx_match_and_get_data(Path ++ [VHostPath, ?STAR_STAR]),
    maps:values(Map).

list_durable_queues(VHost) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> list_queues_with_possible_retry_in_mnesia(
                 fun() ->
                         Pattern = amqqueue:pattern_match_on_name(rabbit_misc:r(VHost, queue)),
                         list_in_mnesia(rabbit_durable_queue, Pattern)
                 end)
      end,
      fun() -> list_queues_with_possible_retry_in_khepri(
                 fun() ->
                         list_in_khepri(khepri_durable_queues_path() ++ [VHost, ?STAR_STAR])
                 end)
      end).

list_queue_names() ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> list_names_in_mnesia(rabbit_queue) end,
      fun() -> list_names_in_khepri(khepri_queues_path()) end).

count_queues() ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> count_in_mnesia(rabbit_queue) end,
      fun() -> count_in_khepri(khepri_queues_path()) end).

count_queues(VHost) ->
    try
        list_queues_for_count(VHost)
    catch _:Err ->
            rabbit_log:error("Failed to fetch number of queues in vhost ~p:~n~p",
                             [VHost, Err]),
            0
    end.

delete_queue(QueueName, Reason) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              delete_queue_in_mnesia(QueueName, Reason)
      end,
      fun() ->
              delete_queue_in_khepri(QueueName, Reason)
      end).

internal_delete_queue(QueueName, OnlyDurable, Reason) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              internal_delete_queue_in_mnesia(QueueName, OnlyDurable, Reason)
      end,
      fun() ->
              internal_delete_queue_in_khepri(QueueName, OnlyDurable, Reason)
      end).

delete_transient_queues(Queues) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              rabbit_misc:execute_mnesia_transaction(
                fun () -> [{QName, delete_transient_queue_in_mnesia(QName)} || QName <- Queues] end
               )
      end,
      fun() ->
              rabbit_khepri:transaction(
                fun() ->
                        [{QName, delete_transient_queue_in_khepri(QName)} || QName <- Queues]
                end, rw)
      end).

store_durable_queue(Q) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              rabbit_misc:execute_mnesia_transaction(
                fun() ->
                        mnesia:write(rabbit_durable_queue, Q, write)
                end)
      end,
      fun() ->
              Path = khepri_durable_queue_path(amqqueue:get_name(Q)),
              case rabbit_khepri:put(Path, Q) of
                  {ok, _} -> ok;
                  Error   -> Error
              end
      end).

lookup_queue(Name) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> lookup({rabbit_queue, Name}, mnesia) end,
      fun() -> lookup(khepri_queue_path(Name), khepri) end).

lookup_durable_queue(Name) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> lookup({rabbit_durable_queue, Name}, mnesia) end,
      fun() -> lookup(khepri_durable_queue_path(Name), khepri) end).

%% TODO this should be internal, it's here because of mirrored queues
lookup_queue_in_khepri_tx(Name) ->
    lookup_tx_in_khepri(khepri_queue_path(Name)).

lookup_queues(Names) when is_list(Names) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> lookup_many(rabbit_queue, Names, mnesia) end,
      fun() -> lookup_many(fun khepri_queue_path/1, Names, khepri) end);
lookup_queues(Name) ->
    lookup_queue(Name).

lookup_durable_queues(Names) when is_list(Names) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> lookup_many(rabbit_durable_queue, Names, mnesia) end,
      fun() -> lookup_many(fun khepri_durable_queue_path/1, Names, khepri) end);
lookup_durable_queues(Name) ->
    lookup_durable_queue(Name).

update_queue(Name, Fun) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> rabbit_misc:execute_mnesia_transaction(
                 fun() ->
                         update_queue_in_mnesia(Name, Fun)
                 end)
      end,
      fun() -> rabbit_khepri:transaction(
                 fun() ->
                         update_queue_in_khepri(Name, Fun)
                 end, rw)
      end).

store_queue(DurableQ, Q) ->
    QName = amqqueue:get_name(Q),
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              rabbit_misc:execute_mnesia_transaction(
                fun () ->
                        case ?amqqueue_is_durable(Q) of
                            true ->
                                ok = mnesia:write(rabbit_durable_queue, DurableQ, write);
                            false ->
                                ok
                        end,
                        ok = mnesia:write(rabbit_queue, Q, write)
                end)
      end,
      fun() ->
              Path = khepri_queue_path(QName),
              DurablePath = khepri_durable_queue_path(QName),
              rabbit_khepri:transaction(
                fun() ->
                        case ?amqqueue_is_durable(Q) of
                            true ->
                                store_in_khepri(DurablePath, DurableQ);
                            false ->
                                ok
                        end,
                        store_in_khepri(Path, Q)
                end)
      end).

store_queue_without_recover(DurableQ, Q) ->
    QueueName = amqqueue:get_name(Q),
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              rabbit_misc:execute_mnesia_transaction(
                fun () ->
                        case mnesia:wread({rabbit_queue, QueueName}) of
                            [] ->
                                case not_found_or_absent_queue_in_mnesia(QueueName) of
                                    not_found           ->
                                        case ?amqqueue_is_durable(Q) of
                                            true ->
                                                ok = mnesia:write(rabbit_durable_queue, DurableQ, write);
                                            false ->
                                                ok
                                        end,
                                        ok = mnesia:write(rabbit_queue, Q, write),
                                        {created, Q};
                                    {absent, _Q, _} = R ->
                                        R
                                end;
                            [ExistingQ] ->
                                {existing, ExistingQ}
                        end
                end)
      end,
      fun() ->
              Path = khepri_queue_path(QueueName),
              DurablePath = khepri_durable_queue_path(QueueName),
              rabbit_khepri:transaction(
                fun() ->
                        case khepri_tx:get(Path) of
                            {ok, #{Path := #{data := ExistingQ}}} ->
                                khepri_tx:abort({existing, ExistingQ});
                            _ ->
                                case not_found_or_absent_queue_in_khepri(QueueName) of
                                    not_found ->
                                        case ?amqqueue_is_durable(DurableQ) of
                                            true ->
                                                store_in_khepri(DurablePath, DurableQ);
                                            false ->
                                                ok
                                        end,
                                        store_in_khepri(Path, Q),
                                        {created, DurableQ};
                                    {absent, _, _} = R ->
                                        khepri_tx:abort(R)
                                end
                        end
                end)
      end).

store_queue_dirty(Q) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              ok = mnesia:dirty_write(rabbit_queue, rabbit_queue_decorator:set(Q))
      end,
      fun() ->
              Path = khepri_queue_path(amqqueue:get_name(Q)),
              case rabbit_khepri:put(Path, Q) of
                  {ok, _} -> ok;
                  Error -> throw(Error)
              end
      end).

store_queues(Qs) ->
    %% TODO only durable or both?
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              rabbit_misc:execute_mnesia_transaction(
                fun() ->
                        [ok = mnesia:write(rabbit_durable_queue, Q, write) || Q <- Qs]
                end)
      end,
      fun() ->
              rabbit_khepri:transaction(
                fun() ->
                        [begin
                             Path = khepri_durable_queue_path(amqqueue:get_name(Q)),
                             store_in_khepri(Path, Q)
                         end || Q <- Qs]
                end)
      end).

update_queue_decorators(Name) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> update_queue_decorators_in_mnesia(Name) end,
      fun() -> update_queue_decorators_in_khepri(Name) end).


%% Routing - HOT CODE PATH

match_bindings(SrcName, Match) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              MatchHead = #route{binding = #binding{source      = SrcName,
                                                    _           = '_'}},
              Routes = ets:select(rabbit_route, [{MatchHead, [], [['$_']]}]),
              [Dest || [#route{binding = Binding = #binding{destination = Dest}}] <-
                           Routes, Match(Binding)]
      end,
      fun() ->
              Data = match_source_in_khepri(SrcName),
              Bindings = lists:foldl(fun(#{bindings := SetOfBindings}, Acc) ->
                                             sets:to_list(SetOfBindings) ++ Acc
                                     end, [], maps:values(Data)),
              [Dest || Binding = #binding{destination = Dest} <- Bindings, Match(Binding)]
      end).

match_routing_key(SrcName, [RoutingKey]) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              MatchHead = #route{binding = #binding{source      = SrcName,
                                                    destination = '$1',
                                                    key         = RoutingKey,
                                                    _           = '_'}},
              ets:select(rabbit_route, [{MatchHead, [], ['$1']}])
      end,
      fun() ->
              match_source_and_key_in_khepri(SrcName, [RoutingKey])
      end);
match_routing_key(SrcName, [_|_] = RoutingKeys) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              %% Normally we'd call mnesia:dirty_select/2 here, but that is quite
              %% expensive for the same reasons as above, and, additionally, due to
              %% mnesia 'fixing' the table with ets:safe_fixtable/2, which is wholly
              %% unnecessary. According to the ets docs (and the code in erl_db.c),
              %% 'select' is safe anyway ("Functions that internally traverse over a
              %% table, like select and match, will give the same guarantee as
              %% safe_fixtable.") and, furthermore, even the lower level iterators
              %% ('first' and 'next') are safe on ordered_set tables ("Note that for
              %% tables of the ordered_set type, safe_fixtable/2 is not necessary as
              %% calls to first/1 and next/2 will always succeed."), which
              %% rabbit_route is.
              MatchHead = #route{binding = #binding{source      = SrcName,
                                                    destination = '$1',
                                                    key         = '$2',
                                                    _           = '_'}},
              Conditions = [list_to_tuple(['orelse' | [{'=:=', '$2', RKey} ||
                                                          RKey <- RoutingKeys]])],
              ets:select(rabbit_route, [{MatchHead, Conditions, ['$1']}])
      end,
      fun() ->
              match_source_and_key_in_khepri(SrcName, RoutingKeys)
      end).

%% Listeners
add_listener(Listener) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              ok = mnesia:dirty_write(rabbit_listener, Listener)
      end,
      fun() -> add_listener_in_khepri(Listener) end).

list_listeners(Node) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              mnesia:dirty_read(rabbit_listener, Node)
      end,
      fun() ->
              case rabbit_khepri:get_data(khepri_listener_path(Node)) of
                  {ok, Set} -> sets:to_list(Set);
                  _ -> []
              end
      end).

delete_listeners(Node) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              ok = mnesia:dirty_delete(rabbit_listener, Node)
      end,
      fun() ->
              rabbit_khepri:delete(khepri_listener_path(Node))
      end).

delete_listener(#listener{node = Node} = Listener) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              ok = mnesia:dirty_delete_object(rabbit_listener, Listener)
      end,
      fun() ->
              rabbit_khepri:transaction(
                fun() ->
                        Path = khepri_listener_path(Node),
                        case khepri_tx:get_data(Listener) of
                            {ok, Set0} ->
                                Set = sets:del_element(Listener, Set0),
                                case sets:is_empty(Set) of
                                    true ->
                                        case khepri_tx:delete(Path) of
                                            {ok, _} -> ok;
                                            Error -> khepri_tx:abort(Error)
                                        end;
                                    false ->
                                        case khepri_tx:put(Path, Set) of
                                            {ok, _} -> ok;
                                            Error -> khepri_tx:abort(Error)
                                        end
                                end;
                            _ ->
                                ok
                        end
                end)
      end).

list_listeners(Node, Protocol) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              rabbit_misc:execute_mnesia_transaction(
                fun() ->
                        MatchSpec = #listener{
                                       node = Node,
                                       protocol = Protocol,
                                       _ = '_'
                                      },
                        case mnesia:match_object(rabbit_listener, MatchSpec, read) of
                            []    -> undefined;
                            [Row] -> Row
                        end
                end)
      end,
      fun() ->
              Path = khepri_listener_path(Node),
              {ok, Set} = rabbit_khepri:get_data(Path),
              Ls = sets:fold(fun(#listener{protocol = P} = Listener, Acc) when P == Protocol ->
                                     [Listener | Acc];
                                (_, Acc) ->
                                     Acc
                             end, [], Set),
              case Ls of
                  [] -> undefined;
                  [L] -> L
              end
      end).

%% Feature flags
%% --------------------------------------------------------------

%% Mnesia contains two tables if an exchange has been recovered:
%% rabbit_exchange (ram) and rabbit_durable_exchange (disc).
%% As all data in Khepri is persistent, there is no point on
%% having ram and data entries. We use the 'persistent' flag on
%% the record to select the exchanges to be recovered or deleted
%% on start-up. Any other query can use this flag to select
%% the exchange type needed.
%% How do we then transform data from mnesia to khepri when
%% the feature flag is enabled?
%% Let's create the Khepri entry if it does not already exist.
%% If ram table is migrated first, the record will be moved as is.
%% If the disc table is migrated first, the record is updated with
%% the exchange decorators.
mnesia_write_exchange_to_khepri(
  #exchange{name = Resource} = Exchange) ->
    Path = khepri_exchange_path(Resource),
    case rabbit_khepri:create(Path, Exchange) of
        {ok, _} -> ok;
        {error, {mismatching_node, _}} -> ok;
        Error -> throw(Error)
    end.

mnesia_write_durable_exchange_to_khepri(
  #exchange{name = Resource} = Exchange0) ->
    Path = khepri_exchange_path(Resource),
    Exchange = rabbit_exchange_decorator:set(Exchange0),
    case rabbit_khepri:create(Path, Exchange) of
        {ok, _} -> ok;
        {error, {mismatching_node, _}} -> ok;
        Error -> throw(Error)
    end.

mnesia_write_exchange_serial_to_khepri(
  #exchange_serial{name = Resource} = Exchange) ->
    Path = khepri_path:combine_with_conditions(khepri_exchange_serial_path(Resource),
                                               [#if_node_exists{exists = false}]),
    Extra = #{keep_while => #{khepri_exchange_path(Resource) => #if_node_exists{exists = true}}},
    case rabbit_khepri:put(Path, Exchange, Extra) of
        {ok, _} -> ok;
        Error -> throw(Error)
    end.

%% There is a single khepri entry for exchanges. Clear it anyway.
clear_exchange_data_in_khepri() ->
    clear_durable_exchange_data_in_khepri().

clear_durable_exchange_data_in_khepri() ->
    Path = khepri_exchanges_path(),
    case rabbit_khepri:delete(Path) of
        {ok, _} -> ok;
        Error -> throw(Error)
    end.

clear_exchange_serial_data_in_khepri() ->
    Path = khepri_exchange_serials_path(),
    case rabbit_khepri:delete(Path) of
        {ok, _} -> ok;
        Error -> throw(Error)
    end.

mnesia_write_route_to_khepri(#route{binding = Binding})->
    store_binding(Binding, transient).

mnesia_write_durable_route_to_khepri(#route{binding = Binding})->
    store_binding(Binding, durable).

mnesia_write_semi_durable_route_to_khepri(#route{binding = Binding})->
    store_binding(Binding, semi_durable).

mnesia_write_reverse_route_to_khepri(_)->
    ok.

clear_route_in_khepri() ->
    Path = khepri_routes_path(),
    RoutingPath = khepri_routing_path(),
    case rabbit_khepri:delete(Path) of
        {ok, _} ->
            case rabbit_khepri:delete(RoutingPath) of
                {ok, _} -> ok;
                Error -> throw(Error)
            end;
        Error -> throw(Error)
    end.

mnesia_write_queue_to_khepri(Q) ->
    Path = khepri_queue_path(amqqueue:get_name(Q)),
    case rabbit_khepri:put(Path, Q) of
        {ok, _} -> ok;
        Error -> throw(Error)
    end.

mnesia_write_durable_queue_to_khepri(Q) ->
    Path = khepri_durable_queue_path(amqqueue:get_name(Q)),
    case rabbit_khepri:put(Path, Q) of
        {ok, _} -> ok;
        Error -> throw(Error)
    end.

mnesia_delete_queue_to_khepri(Q) when ?is_amqqueue(Q) ->
    mnesia_delete_queue_to_khepri(amqqueue:get_name(Q));
mnesia_delete_queue_to_khepri(#resource{} = Name) ->
    Path = khepri_queue_path(Name),
    case rabbit_khepri:delete(Path) of
        {ok, _} -> ok;
        Error -> throw(Error)
    end.

mnesia_delete_durable_queue_to_khepri(Q) when ?is_amqqueue(Q) ->
    mnesia_delete_durable_queue_to_khepri(amqqueue:get_name(Q));
mnesia_delete_durable_queue_to_khepri(#resource{} = Name) ->
    Path = khepri_durable_queue_path(Name),
    case rabbit_khepri:delete(Path) of
        {ok, _} -> ok;
        Error -> throw(Error)
    end.

clear_queue_data_in_khepri() ->
    Path = khepri_queues_path(),
    case rabbit_khepri:delete(Path) of
        {ok, _} -> ok;
        Error -> throw(Error)
    end.

clear_durable_queue_data_in_khepri() ->
    Path = khepri_durable_queues_path(),
    case rabbit_khepri:delete(Path) of
        {ok, _} -> ok;
        Error -> throw(Error)
    end.

mnesia_write_listener_to_khepri(Listener) ->
    add_listener_in_khepri(Listener).

clear_listener_data_in_khepri() ->
    Path = khepri_listeners_path(),
    case rabbit_khepri:delete(Path) of
        {ok, _} -> ok;
        Error -> throw(Error)
    end.

%% Internal
%% --------------------------------------------------------------
list_in_mnesia(Table, Match) ->
    %% Not dirty_match_object since that would not be transactional when used in a
    %% tx context
    mnesia:async_dirty(fun () -> mnesia:match_object(Table, Match, read) end).

list_in_khepri(Path) ->
    case rabbit_khepri:match_and_get_data(Path) of
        {ok, Map} -> maps:values(Map);
        _         -> []
    end.

count_in_mnesia(Table) ->
    mnesia:table_info(Table, size).

count_in_khepri(Path) ->
    case rabbit_khepri:match(Path ++ [?STAR_STAR]) of
        {ok, Map} -> maps:size(Map);
        _            -> 0
    end.

list_names_in_mnesia(Table) ->
    mnesia:dirty_all_keys(Table).

list_names_in_khepri(Path) ->
    case rabbit_khepri:match_and_get_data(Path ++ [?STAR_STAR]) of
        {ok, Map} -> maps:keys(Map);
        _            -> []
    end.

lookup(Name, mnesia) ->
    rabbit_misc:dirty_read(Name);
lookup(Path, khepri) ->
    case rabbit_khepri:get_data(Path) of
        {ok, X} -> {ok, X};
        _ -> {error, not_found}
    end.

lookup_many(Table, [Name], mnesia) -> ets:lookup(Table, Name);
lookup_many(Table, Names, mnesia) when is_list(Names) ->
    %% Normally we'd call mnesia:dirty_read/1 here, but that is quite
    %% expensive for reasons explained in rabbit_misc:dirty_read/1.
    lists:append([ets:lookup(Table, Name) || Name <- Names]);
lookup_many(Fun, Names, khepri) when is_list(Names) ->
    lists:foldl(fun(Name, Acc) ->
                        case lookup(Fun(Name), khepri) of
                            {ok, X} -> [X | Acc];
                            _ -> Acc
                        end
                end, [], Names).

update_exchange_decorators(Name, mnesia) ->
    rabbit_misc:execute_mnesia_transaction(
      fun() ->
              case mnesia:wread({rabbit_exchange, Name}) of
                  [X] -> store_ram_exchange(X),
                         ok;
                  []  -> ok
              end
      end);
update_exchange_decorators(#resource{virtual_host = VHost, name = Name} = XName, khepri) ->
    Path = khepri_exchange_path(XName),
    rabbit_kepri_misc:retry(
      fun () ->
              case rabbit_khepri:get(Path) of
                  {ok, #{Path := #{data := X, payload_version := Vsn}}} ->
                      X1 = rabbit_exchange_decorator:set(X),
                      Conditions = #if_all{conditions = [Name, #if_payload_version{version = Vsn}]},
                      UpdatePath = khepri_exchanges_path() ++ [VHost, Conditions],
                      rabbit_khepri:put(UpdatePath, X1);
                  _ ->
                      ok
              end
      end).

store_exchange_in_mnesia(X = #exchange{durable = true}) ->
    mnesia:write(rabbit_durable_exchange, X#exchange{decorators = undefined},
                 write),
    store_ram_exchange(X);
store_exchange_in_mnesia(X = #exchange{durable = false}) ->
    store_ram_exchange(X).

store_exchange_in_khepri(X) ->
    Path = khepri_exchange_path(X#exchange.name),
    {ok, _} = khepri_tx:put(Path, X),
    X.

store_ram_exchange(X) ->
    X1 = rabbit_exchange_decorator:set(X),
    ok = mnesia:write(rabbit_exchange, X1, write),
    X1.

create_exchange_in_mnesia(Name, X) ->
    case lookup_tx_in_mnesia(Name) of
        [] ->
            {new, store_exchange_in_mnesia(X)};
        [ExistingX] ->
            {existing, ExistingX}
    end.

create_exchange_in_khepri(Path, X) ->
    case lookup_tx_in_khepri(Path) of
        [] ->
            {new, store_exchange_in_khepri(X)};
        [ExistingX] ->
            {existing, ExistingX}
    end.

execute_transaction(TxFun, PrePostCommitFun, mnesia) ->
    case mnesia:is_transaction() of
        true  -> throw(unexpected_transaction);
        false -> ok
    end,
    PrePostCommitFun(rabbit_misc:execute_mnesia_transaction(
                       fun () ->
                               Result = TxFun(),
                               PrePostCommitFun(Result, true)
                       end), false);
execute_transaction(TxFun, PostCommitFun, khepri) ->
    execute_khepri_transaction(TxFun, PostCommitFun).

execute_khepri_transaction(TxFun, PostCommitFun) ->
    case khepri_tx:is_transaction() of
        true  -> throw(unexpected_transaction);
        false -> ok
    end,
    PostCommitFun(rabbit_khepri:transaction(
                    fun () ->
                            TxFun()
                    end, rw), all).

delete_exchange_fun(Name, IfUnused, PrePostCommitFun, mnesia) ->
    fun() ->
            DeletionFun = case IfUnused of
                              true  -> fun conditional_delete_exchange_in_mnesia/2;
                              false -> fun unconditional_delete_exchange_in_mnesia/2
                          end,
            execute_transaction(fun() ->
                                        case lookup_tx_in_mnesia(Name) of
                                            [X] -> DeletionFun(X, false);
                                            [] -> {error, not_found}
                                        end
                                end, PrePostCommitFun, mnesia)
    end;
delete_exchange_fun(Name, IfUnused, PrePostCommitFun, khepri) ->
    fun() ->
            DeletionFun = case IfUnused of
                              true  -> fun conditional_delete_exchange_in_khepri/2;
                              false -> fun unconditional_delete_exchange_in_khepri/2
                          end,
            execute_transaction(fun() ->
                                        case lookup_tx_in_khepri(Name) of
                                            [X] -> DeletionFun(X, false);
                                            [] -> {error, not_found}
                                        end
                                end, PrePostCommitFun, khepri)
    end.                              

remove_bindings_in_mnesia(X = #exchange{name = XName}, OnlyDurable, RemoveBindingsForSource) ->
    Bindings = case RemoveBindingsForSource of
                   true  -> remove_bindings_for_source_in_mnesia(XName);
                   false -> []
               end,
    {deleted, X, Bindings, remove_bindings_for_destination_in_mnesia(XName, OnlyDurable, fun remove_routes/1)}.

remove_bindings_in_khepri(X = #exchange{name = XName}, OnlyDurable, RemoveBindingsForSource) ->
    Bindings = case RemoveBindingsForSource of
                   true  -> remove_bindings_for_source_in_khepri(XName);
                   false -> []
               end,
    {deleted, X, Bindings, remove_bindings_for_destination_in_khepri(XName, OnlyDurable)}.

map_create_tx(true) -> transaction;
map_create_tx(false) -> none.
    
recover_exchanges(VHost, mnesia) ->
    rabbit_misc:table_filter(
      fun (#exchange{name = XName}) ->
              XName#resource.virtual_host =:= VHost andalso
                  mnesia:read({rabbit_exchange, XName}) =:= []
      end,
      fun (X, Tx) ->
              X1 = case Tx of
                       true  -> store_exchange_in_mnesia(X);
                       false -> rabbit_exchange_decorator:set(X)
                   end,
              %% TODO how do we guarantee that lower down the callback
              %% we use the right transaction type? mnesia/khepri
              rabbit_exchange:callback(X1, create, map_create_tx(Tx), [X1])
      end,
      rabbit_durable_exchange);
recover_exchanges(VHost, khepri) ->
    %% Khepri does not have ram tables, so data will not be lost
    %% once a node is restarted. Thus, a single table with
    %% idempotent recovery is enough. The exchange record already
    %% contains the durable flag and we use it to clean up or recover
    %% records.
    
    DurablePattern = #if_data_matches{pattern = #exchange{durable = true, _ = '_'}},
    DurableExchanges0 = list_in_khepri(khepri_exchanges_path() ++ [VHost, DurablePattern]),
    DurableExchanges = [rabbit_exchange_decorator:set(X) || X <- DurableExchanges0],

    TransientPattern = #if_data_matches{pattern = #exchange{durable = false, _ = '_'}},

    rabbit_khepri:transaction(
      fun() ->
              [_ = store_exchange_in_khepri(X) || X <- DurableExchanges],
              _ = khepri_tx:delete(khepri_exchanges_path() ++ [VHost, TransientPattern])
      end),
    %% TODO once mnesia is gone, this callback should go back to `rabbit_exchange`
    [begin
         rabbit_exchange:callback(X, create, [transaction, none], [X])
     end || X <- DurableExchanges],
    DurableExchanges.

lookup_tx_in_mnesia(Name) ->
    mnesia:wread(Name).

lookup_tx_in_khepri(Path) ->
    case khepri_tx:get(Path) of
        {ok, #{Path := #{data := X}}} -> [X];
        _ -> []
    end.

binding_action_in_mnesia(#binding{source      = SrcName,
                                  destination = DstName}, Fun, ErrFun) ->
    SrcTable = table_for_resource(SrcName),
    DstTable = table_for_resource(DstName),
    rabbit_misc:execute_mnesia_tx_with_tail(
      fun () ->
              case {mnesia:read({SrcTable, SrcName}),
                    mnesia:read({DstTable, DstName})} of
                  {[Src], [Dst]} -> Fun(Src, Dst);
                  {[],    [_]  } -> ErrFun([SrcName]);
                  {[_],   []   } -> ErrFun([DstName]);
                  {[],    []   } -> ErrFun([SrcName, DstName])
              end
      end).

table_for_resource(#resource{kind = exchange}) -> rabbit_exchange;
table_for_resource(#resource{kind = queue})    -> rabbit_queue.

lookup_resources(Src, Dst) ->
    rabbit_khepri:transaction(
      fun() ->
              {lookup_resource_in_khepri_tx(Src), lookup_resource_in_khepri_tx(Dst)}
      end, ro).

lookup_resource_in_khepri_tx(#resource{kind = queue} = Name) ->
    lookup_tx_in_khepri(khepri_queue_path(Name));
lookup_resource_in_khepri_tx(#resource{kind = exchange} = Name) ->
    lookup_tx_in_khepri(khepri_exchange_path(Name)).

exists_binding_in_khepri(Path, Binding) ->
    case khepri_tx:get(Path) of
        {ok, #{Path := #{data := #{bindings := Set}}}} ->
            sets:is_element(Binding, Set);
        _ ->
            false
    end.

not_found({[], [_]}, SrcName, _) ->
    [SrcName];
not_found({[_], []}, _, DstName) ->
    [DstName];
not_found({[], []}, SrcName, DstName) ->
    [SrcName, DstName].

not_found_or_absent_errs_in_mnesia(Names) ->
    Errs = [not_found_or_absent_in_mnesia(Name) || Name <- Names],
    rabbit_misc:const({error, {resources_missing, Errs}}).

not_found_or_absent_errs_in_khepri(Names) ->
    Errs = [not_found_or_absent_in_khepri(Name) || Name <- Names],
    {error, {resources_missing, Errs}}.

absent_errs_only_in_mnesia(Names) ->
    Errs = [E || Name <- Names,
                 {absent, _Q, _Reason} = E <- [not_found_or_absent_in_mnesia(Name)]],
    rabbit_misc:const(case Errs of
                          [] -> ok;
                          _  -> {error, {resources_missing, Errs}}
                      end).

absent_errs_only_in_khepri(Names) ->
    Errs = [E || Name <- Names,
                 {absent, _Q, _Reason} = E <- [not_found_or_absent_in_khepri(Name)]],
    case Errs of
        [] -> ok;
        _  -> {error, {resources_missing, Errs}}
    end.

not_found_or_absent_in_mnesia(#resource{kind = exchange} = Name) ->
    {not_found, Name};
not_found_or_absent_in_mnesia(#resource{kind = queue}    = Name) ->
    case not_found_or_absent_queue_in_mnesia(Name) of
        not_found                 -> {not_found, Name};
        {absent, _Q, _Reason} = R -> R
    end.

not_found_or_absent_queue_in_mnesia(Name) ->
    %% NB: we assume that the caller has already performed a lookup on
    %% rabbit_queue and not found anything
    case mnesia:read({rabbit_durable_queue, Name}) of
        []  -> not_found;
        [Q] -> {absent, Q, nodedown} %% Q exists on stopped node
    end.

not_found_or_absent_in_khepri(#resource{kind = exchange} = Name) ->
    {not_found, Name};
not_found_or_absent_in_khepri(#resource{kind = queue}    = Name) ->
    case not_found_or_absent_queue_in_khepri(Name) of
        not_found                 -> {not_found, Name};
        {absent, _Q, _Reason} = R -> R
    end.

not_found_or_absent_queue_in_khepri(Name) ->
    %% NB: we assume that the caller has already performed a lookup on
    %% rabbit_queue and not found anything
    Path = khepri_durable_queue_path(Name),
    case khepri_tx:get(Path) of
        {ok, #{Path := #{data := Q}}} -> {absent, Q, nodedown}; %% Q exists on stopped node
        _  -> not_found
    end.

not_found_or_absent_queue_dirty(Name) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> not_found_or_absent_queue_dirty_in_mnesia(Name) end,
      fun() ->
              %% This might hit khepri cache, vs a full transaction
              Path = khepri_durable_queue_path(Name),
              case rabbit_khepri:get_data(Path) of
                  {ok, Q} -> {absent, Q, nodedown}; %% Q exists on stopped node
                  _  -> not_found
              end
      end).

not_found_or_absent_queue_dirty_in_mnesia(Name) ->
    %% We should read from both tables inside a tx, to get a
    %% consistent view. But the chances of an inconsistency are small,
    %% and only affect the error kind.
    case rabbit_misc:dirty_read({rabbit_durable_queue, Name}) of
        {error, not_found} -> not_found;
        {ok, Q}            -> {absent, Q, nodedown}
    end.

serial_in_mnesia(false, _) ->
    none;
serial_in_mnesia(true, X) ->
    next_exchange_serial_in_mnesia(X).

serial_in_khepri(false, _) ->
    none;
serial_in_khepri(true, X) ->
    next_exchange_serial_in_khepri(X).

bindings_data(Path, BindingType) ->
    case khepri_tx:get(Path) of
        {ok, #{Path := #{data := Data}}} ->
            Data;
        _ ->
            #{bindings => sets:new(), type => BindingType}
    end.

store_binding(Binding, BindingType) ->
    Path = khepri_route_path(Binding),
    rabbit_khepri:transaction(
      fun() ->
              add_binding_tx(Path, Binding, BindingType)
      end, rw).

add_binding_tx(Path, Binding, BindingType) ->
    Data0 = #{bindings := Set} = bindings_data(Path, BindingType),
    Data = Data0#{bindings => sets:add_element(Binding, Set)},
    {ok, _} = khepri_tx:put(Path, Data),
    add_routing(Binding),
    ok.

add_routing(#binding{destination = Dst} = Binding) ->
    Path = khepri_routing_path(Binding),
    case khepri_tx:get(Path) of
        {ok, #{Path := #{data := Data}}} ->
            {ok, _} = khepri_tx:put(Path, sets:add_element(Dst, Data));
        _ ->
            {ok, _} = khepri_tx:put(Path, sets:add_element(Dst, sets:new()))
    end.

add_binding_in_mnesia(Binding, ChecksFun) ->
    binding_action_in_mnesia(
      Binding,
      fun (Src, Dst) ->
              case ChecksFun(Src, Dst) of
                  {ok, BindingType} ->
                      case mnesia:read({rabbit_route, Binding}) of
                          []  ->
                              ok = sync_route(#route{binding = Binding}, BindingType,
                                              fun mnesia:write/3),
                              rabbit_exchange:callback(Src, add_binding, transaction, [Src, Binding]),
                              MaybeSerial = rabbit_exchange:serialise_events(Src),
                              Serial = serial_in_mnesia(MaybeSerial, Src),
                              fun () ->
                                      rabbit_exchange:callback(Src, add_binding, Serial, [Src, Binding])
                              end;
                          [_] -> fun () -> ok end
                      end;
                  {error, _} = Err ->
                      rabbit_misc:const(Err)
              end
      end, fun not_found_or_absent_errs_in_mnesia/1).

add_binding_in_khepri(#binding{source = SrcName,
                               destination = DstName} = Binding, ChecksFun) ->
    case lookup_resources(SrcName, DstName) of
        {[Src], [Dst]} ->
            case ChecksFun(Src, Dst) of
                {ok, BindingType} ->
                    Path = khepri_route_path(Binding),
                    MaybeSerial = rabbit_exchange:serialise_events(Src),
                    Serial = rabbit_khepri:transaction(
                               fun() ->
                                       add_binding_tx(Path, Binding, BindingType),
                                       serial_in_khepri(MaybeSerial, Src)
                               end, rw),
                    rabbit_exchange:callback(Src, add_binding, [transaction, Serial], [Src, Binding]),
                    ok;
                {error, _} = Err ->
                    Err
            end;
        Errs ->
            not_found_or_absent_errs_in_khepri(not_found(Errs, SrcName, DstName))
    end.

remove_binding_in_mnesia(Binding, ChecksFun) ->
    binding_action_in_mnesia(
      Binding,
      fun (Src, Dst) ->
              lock_resource(Src, read),
              lock_resource(Dst, read),
              case mnesia:read(rabbit_route, Binding, write) of
                  [] -> case mnesia:read(rabbit_durable_route, Binding, write) of
                            [] -> rabbit_misc:const(ok);
                            %% We still delete the binding and run
                            %% all post-delete functions if there is only
                            %% a durable route in the database
                            _  -> remove_binding_in_mnesia(Src, Dst, Binding)
                        end;
                  _  -> case ChecksFun(Dst) of
                            ok               -> remove_binding_in_mnesia(Src, Dst, Binding);
                            {error, _} = Err -> rabbit_misc:const(Err)
                        end
              end
      end, fun absent_errs_only_in_mnesia/1).

remove_binding_in_mnesia(Src, Dst, B) ->
    ok = sync_route(#route{binding = B}, rabbit_binding:binding_type(Src, Dst),
                    fun delete/3),
    Deletions0 = maybe_auto_delete_exchange_in_mnesia(
                   B#binding.source, [B], rabbit_binding:new_deletions(), false),
    Deletions = rabbit_binding:process_deletions(Deletions0, true),
    fun() -> rabbit_binding:process_deletions(Deletions, false) end.

sync_route(Route, durable, Fun) ->
    ok = Fun(rabbit_durable_route, Route, write),
    sync_route(Route, semi_durable, Fun);

sync_route(Route, semi_durable, Fun) ->
    ok = Fun(rabbit_semi_durable_route, Route, write),
    sync_route(Route, transient, Fun);

sync_route(Route, transient, Fun) ->
    sync_transient_route(Route, Fun).

sync_transient_route(Route, Fun) ->
    ok = Fun(rabbit_route, Route, write),
    ok = Fun(rabbit_reverse_route, rabbit_binding:reverse_route(Route), write).

remove_transient_routes(Routes) ->
    [begin
         ok = sync_transient_route(R, fun delete/3),
         R#route.binding
     end || R <- Routes].

remove_routes(Routes) ->
    %% This partitioning allows us to suppress unnecessary delete
    %% operations on disk tables, which require an fsync.
    {RamRoutes, DiskRoutes} =
        lists:partition(fun (R) -> mnesia:read(
                                     rabbit_durable_route, R#route.binding, read) == [] end,
                        Routes),
    {RamOnlyRoutes, SemiDurableRoutes} =
        lists:partition(fun (R) -> mnesia:read(
                                     rabbit_semi_durable_route, R#route.binding, read) == [] end,
                        RamRoutes),
    %% Of course the destination might not really be durable but it's
    %% just as easy to try to delete it from the semi-durable table
    %% than check first
    [ok = sync_route(R, durable, fun delete/3) ||
        R <- DiskRoutes],
    [ok = sync_route(R, semi_durable, fun delete/3) ||
        R <- SemiDurableRoutes],
    [ok = sync_route(R, transient, fun delete/3) ||
        R <- RamOnlyRoutes],
    [R#route.binding || R <- Routes].

delete(Tab, #route{binding = B}, LockKind) ->
    mnesia:delete(Tab, B, LockKind);
delete(Tab, #reverse_route{reverse_binding = B}, LockKind) ->
    mnesia:delete(Tab, B, LockKind).

remove_binding_in_khepri(#binding{source = SrcName,
                                  destination = DstName} = Binding, ChecksFun) ->
    Path = khepri_route_path(Binding),
    case rabbit_khepri:transaction(
           fun () ->
                   case {lookup_resource_in_khepri_tx(SrcName),
                         lookup_resource_in_khepri_tx(DstName)} of
                       {[_Src], [Dst]} ->
                           case exists_binding_in_khepri(Path, Binding) of
                               false ->
                                   ok;
                               true ->
                                   case ChecksFun(Dst) of
                                       ok ->
                                           ok = delete_binding_in_khepri(Binding),
                                           ok = delete_routing(Binding),
                                           maybe_auto_delete_exchange_in_khepri(Binding#binding.source, [Binding], rabbit_binding:new_deletions(), false);
                                       {error, _} = Err ->
                                           Err
                                   end
                           end;
                       Errs ->
                           absent_errs_only_in_khepri(not_found(Errs, SrcName, DstName))
                   end
           end) of
        ok ->
            ok;
        {error, _} = Err ->
            Err;
        Deletions ->
            rabbit_binding:process_deletions(Deletions, all)
    end.

maybe_auto_delete_exchange_in_mnesia(XName, Bindings, Deletions, OnlyDurable) ->
    {Entry, Deletions1} =
        case mnesia:read({case OnlyDurable of
                              true  -> rabbit_durable_exchange;
                              false -> rabbit_exchange
                          end, XName}) of
            []  -> {{undefined, not_deleted, Bindings}, Deletions};
            [X] -> case maybe_auto_delete_exchange_in_mnesia(X, OnlyDurable) of
                       not_deleted ->
                           {{X, not_deleted, Bindings}, Deletions};
                       {deleted, Deletions2} ->
                           {{X, deleted, Bindings},
                            rabbit_binding:combine_deletions(Deletions, Deletions2)}
                   end
        end,
    rabbit_binding:add_deletion(XName, Entry, Deletions1).

maybe_auto_delete_exchange_in_khepri(XName, Bindings, Deletions, OnlyDurable) ->
    {Entry, Deletions1} =
        case maybe_auto_delete_exchange_in_khepri(XName, OnlyDurable) of
            {not_deleted, X} ->
                {{X, not_deleted, Bindings}, Deletions};
            {deleted, X, Deletions2} ->
                {{X, deleted, Bindings},
                 rabbit_binding:combine_deletions(Deletions, Deletions2)}
        end,
    rabbit_binding:add_deletion(XName, Entry, Deletions1).

maybe_auto_delete_exchange_in_khepri(XName, OnlyDurable) ->
    case lookup_tx_in_khepri(khepri_exchange_path(XName)) of
        [] ->
            {not_deleted, undefined};
        [#exchange{auto_delete = false} = X] ->
            {not_deleted, X};
        [#exchange{auto_delete = true} = X] ->
            case conditional_delete_exchange_in_khepri(X, OnlyDurable) of
                {error, in_use}             -> {not_deleted, X};
                {deleted, X, [], Deletions} -> {deleted, X, Deletions}
            end
    end.

-spec maybe_auto_delete_exchange_in_mnesia
        (rabbit_types:exchange(), boolean())
        -> 'not_deleted' | {'deleted', rabbit_binding:deletions()}.
maybe_auto_delete_exchange_in_mnesia(#exchange{auto_delete = false}, _OnlyDurable) ->
    not_deleted;
maybe_auto_delete_exchange_in_mnesia(#exchange{auto_delete = true} = X, OnlyDurable) ->
    case conditional_delete_exchange_in_mnesia(X, OnlyDurable) of
        {error, in_use}             -> not_deleted;
        {deleted, X, [], Deletions} -> {deleted, Deletions}
    end.

conditional_delete_exchange_in_khepri(X = #exchange{name = XName}, OnlyDurable) ->
    case binding_has_for_source_in_khepri(XName) of
        false  -> delete_exchange_in_khepri(X, OnlyDurable, false);
        true   -> {error, in_use}
    end.

conditional_delete_exchange_in_mnesia(X = #exchange{name = XName}, OnlyDurable) ->
    case binding_has_for_source_in_mnesia(XName) of
        false  -> delete_exchange_in_mnesia(X, OnlyDurable, false);
        true   -> {error, in_use}
    end.

unconditional_delete_exchange_in_mnesia(X, OnlyDurable) ->
    delete_exchange_in_mnesia(X, OnlyDurable, true).

unconditional_delete_exchange_in_khepri(X, OnlyDurable) ->
    delete_exchange_in_khepri(X, OnlyDurable, true).

delete_binding_in_mnesia(Binding) ->
    mnesia:dirty_delete(rabbit_durable_route, Binding),
    mnesia:dirty_delete(rabbit_semi_durable_route, Binding),
    mnesia:dirty_delete(rabbit_reverse_route,
                        rabbit_binding:reverse_binding(Binding)),
    mnesia:dirty_delete(rabbit_route, Binding).

delete_binding_in_khepri(Binding) ->
    Path = khepri_route_path(Binding),
    case khepri_tx:get(Path) of
        {ok, #{Path := #{data := #{bindings := Set0} = Data}}} ->
            Set = sets:del_element(Binding, Set0),
            case sets:is_empty(Set) of
                true ->
                    {ok, _} = khepri_tx:delete(Path);
                false ->
                    {ok, _} = khepri_tx:put(Path, Data#{bindings => Set})
            end;
        _ ->
            ok
    end,
    ok.

delete_routing(#binding{destination = Dst} = Binding) ->
    Path = khepri_routing_path(Binding),
    case khepri_tx:get(Path) of
        {ok, #{Path := #{data := Data0}}} ->
            Data = sets:del_element(Dst, Data0),
            case sets:is_empty(Data) of
                true ->
                    {ok, _} = khepri_tx:delete(Path);
                false ->
                    {ok, _} = khepri_tx:put(Path, Data)
            end;
        _ ->
            ok
    end,
    ok.

%% Instead of locking entire table on remove operations we can lock the
%% affected resource only.
lock_resource(Name) -> lock_resource(Name, write).

lock_resource(Name, LockKind) ->
    mnesia:lock({global, Name, mnesia:table_info(rabbit_route, where_to_write)},
                LockKind).

-spec binding_has_for_source_in_mnesia(rabbit_types:binding_source()) -> boolean().

binding_has_for_source_in_mnesia(SrcName) ->
    Match = #route{binding = #binding{source = SrcName, _ = '_'}},
    %% we need to check for semi-durable routes (which subsumes
    %% durable routes) here too in case a bunch of routes to durable
    %% queues have been removed temporarily as a result of a node
    %% failure
    contains(rabbit_route, Match) orelse
        contains(rabbit_semi_durable_route, Match).

contains(Table, MatchHead) ->
    continue(mnesia:select(Table, [{MatchHead, [], ['$_']}], 1, read)).

continue('$end_of_table')    -> false;
continue({[_|_], _})         -> true;
continue({[], Continuation}) -> continue(mnesia:select(Continuation)).

binding_has_for_source_in_khepri(SrcName) ->
    (not maps:is_empty(match_source_in_khepri(SrcName, transient)))
        orelse (not maps:is_empty(match_source_in_khepri(SrcName, semi_durable))).

destination_from_khepri_path([?MODULE, _Type, VHost, _Src, Kind, Dst | _]) ->
    #resource{virtual_host = VHost, kind = Kind, name = Dst}.

source_from_khepri_path([?MODULE, _Type, VHost, Src | _]) ->
    #resource{virtual_host = VHost, kind = exchange, name = Src}.

match_source_in_khepri_tx(#resource{virtual_host = VHost, name = Name}) ->
    Path = khepri_routes_path() ++ [VHost, Name, ?STAR_STAR],
    {ok, Map} = rabbit_khepri:tx_match_and_get_data(Path),
    Map.

match_source_in_khepri(#resource{virtual_host = VHost, name = Name}) ->
    Path = khepri_routes_path() ++ [VHost, Name, ?STAR_STAR],
    {ok, Map} = rabbit_khepri:match_and_get_data(Path),
    Map.

match_source_and_key_in_khepri(Src, ['_']) ->
    Path = khepri_routing_path(Src, ?STAR_STAR),
    case rabbit_khepri:match_and_get_data(Path) of
        {ok, Map} ->
            maps:fold(fun(_, Dsts, Acc) ->
                              sets:to_list(Dsts) ++ Acc
                      end, [], Map);
        {error, {node_not_found, _}} ->
            []
    end;
match_source_and_key_in_khepri(Src, RoutingKeys) ->
    lists:foldl(
      fun(RK, Acc) ->
              Path = khepri_routing_path(Src, RK),
              %% Don't use transaction if we want to hit the cache
              case rabbit_khepri:get_data(Path) of
                  {ok, Dsts} ->
                      sets:to_list(Dsts) ++ Acc;
                  {error, {node_not_found, _}} ->
                      Acc
              end
      end, [], RoutingKeys).

match_source_in_khepri(#resource{virtual_host = VHost, name = Name}, Type) ->
    Path = khepri_routes_path() ++ [VHost, Name]
        ++ [#if_all{conditions = [?STAR_STAR, #if_data_matches{pattern = #{type => Type}}]}],
    {ok, Map} = rabbit_khepri:tx_match_and_get_data(Path),
    Map.

match_destination_in_khepri(#resource{virtual_host = VHost, kind = Kind, name = Name}) ->
    Path = khepri_routes_path() ++ [VHost, ?STAR, Kind, Name, ?STAR_STAR],
    {ok, Map} = rabbit_khepri:tx_match_and_get_data(Path),
    Map.

match_destination_in_khepri(#resource{virtual_host = VHost, kind = Kind, name = Name}, Type) ->
    Path = khepri_routes_path() ++ [VHost, ?STAR, Kind, Name]
        ++ [#if_all{conditions = [?STAR_STAR, #if_data_matches{pattern = #{type => Type}}]}],
    {ok, Map} = rabbit_khepri:tx_match_and_get_data(Path),
    Map.

match_source_and_destination_in_khepri(#resource{virtual_host = VHost, name = Name},
                                       #resource{kind = Kind, name = DstName}) ->
    Path = khepri_routes_path() ++ [VHost, Name, Kind, DstName, ?STAR_STAR],
    list_in_khepri(Path).

remove_bindings_for_destination_in_mnesia(DstName, OnlyDurable, Fun) ->
    lock_resource(DstName),
    MatchFwd = #route{binding = #binding{destination = DstName, _ = '_'}},
    MatchRev = rabbit_binding:reverse_route(MatchFwd),
    Routes = case OnlyDurable of
                 false ->
                        [rabbit_binding:reverse_route(R) ||
                              R <- mnesia:dirty_match_object(
                                     rabbit_reverse_route, MatchRev)];
                 true  -> lists:usort(
                            mnesia:dirty_match_object(
                              rabbit_durable_route, MatchFwd) ++
                                mnesia:dirty_match_object(
                                  rabbit_semi_durable_route, MatchFwd))
             end,
    Bindings = Fun(Routes),
    rabbit_binding:group_bindings_fold(fun maybe_auto_delete_exchange_in_mnesia/4,
                                       lists:keysort(#binding.source, Bindings), OnlyDurable).

remove_bindings_for_destination_in_mnesia(DstName, OnlyDurable) ->
    remove_bindings_for_destination_in_mnesia(DstName, OnlyDurable, fun remove_routes/1).

remove_bindings_for_destination_in_khepri(DstName, OnlyDurable) ->
    BindingsMap =
        case OnlyDurable of
            false ->
                TransientBindingsMap = match_destination_in_khepri(DstName),
                maps:foreach(fun(K, _V) -> khepri_tx:delete(K) end, TransientBindingsMap),
                TransientBindingsMap;
            true  ->
                BindingsMap0 = match_destination_in_khepri(DstName, durable),
                BindingsMap1 = match_destination_in_khepri(DstName, semi_durable),
                BindingsMap2 = maps:merge(BindingsMap0, BindingsMap1),
                maps:foreach(fun(K, _V) -> khepri_tx:delete(K) end, BindingsMap2),
                BindingsMap2
        end,
    Bindings = maps:fold(fun(_, #{bindings := Set}, Acc) ->
                                 sets:to_list(Set) ++ Acc
                         end, [], BindingsMap),
    rabbit_binding:group_bindings_fold(fun maybe_auto_delete_exchange_in_khepri/4,
                                       lists:keysort(#binding.source, Bindings), OnlyDurable).

-spec remove_bindings_for_source_in_mnesia(rabbit_types:binding_source()) -> [rabbit_types:binding()].
remove_bindings_for_source_in_mnesia(SrcName) ->
    lock_resource(SrcName),
    Match = #route{binding = #binding{source = SrcName, _ = '_'}},
    remove_routes(
      lists:usort(
        mnesia:dirty_match_object(rabbit_route, Match) ++
            mnesia:dirty_match_object(rabbit_semi_durable_route, Match))).

remove_bindings_for_source_in_khepri(SrcName) ->
    Bindings = match_source_in_khepri_tx(SrcName),
    maps:foreach(fun(K, _V) -> khepri_tx:delete(K) end, Bindings),
    maps:fold(fun(_, #{bindings := Set}, Acc) ->
                      sets:to_list(Set) ++ Acc
              end, [], Bindings).

-spec remove_transient_bindings_for_destination_in_mnesia(rabbit_types:binding_destination()) -> rabbit_binding:deletions().
remove_transient_bindings_for_destination_in_mnesia(DstName) ->
    remove_bindings_for_destination_in_mnesia(DstName, false, fun remove_transient_routes/1).

remove_transient_bindings_for_destination_in_khepri(DstName) ->
    TransientBindingsMap = match_destination_in_khepri(DstName, transient),
    maps:foreach(fun(K, _V) -> khepri_tx:delete(K) end, TransientBindingsMap),
    Bindings = maps:fold(fun(_, #{bindings := Set}, Acc) ->
                                 sets:to_list(Set) ++ Acc
                         end, [], TransientBindingsMap),
    rabbit_binding:group_bindings_fold(fun maybe_auto_delete_exchange_in_khepri/4,
                                       lists:keysort(#binding.source, Bindings), false).

recover_bindings_in_mnesia() ->
    rabbit_misc:execute_mnesia_transaction(
        fun () ->
            mnesia:lock({table, rabbit_durable_route}, read),
            mnesia:lock({table, rabbit_semi_durable_route}, write),
            Routes = rabbit_misc:dirty_read_all(rabbit_durable_route),
            Fun = fun(Route) ->
                mnesia:dirty_write(rabbit_semi_durable_route, Route)
            end,
        lists:foreach(Fun, Routes)
    end).

recover_semi_durable_route_txn(#route{binding = B} = Route, X, mnesia) ->
    MaybeSerial = rabbit_exchange:serialise_events(X),
    execute_transaction(
      fun () ->
              case mnesia:read(rabbit_semi_durable_route, B, read) of
                  [] -> no_recover;
                  _  -> ok = sync_transient_route(Route, fun mnesia:write/3),
                        serial_in_mnesia(MaybeSerial, X)
              end
      end,
      fun (no_recover, _) -> ok;
          (_Serial, true) -> rabbit_exchange:callback(X, add_binding, transaction, [X, B]);
          (Serial, false) -> rabbit_exchange:callback(X, add_binding, Serial, [X, B])
      end,
      mnesia);
recover_semi_durable_route_txn(Path, X, khepri) ->
    MaybeSerial = rabbit_exchange:serialise_events(X),
    execute_transaction(
      fun () ->
              case khepri_tx:get(Path) of
                  {ok, #{Path := #{data := #{bindings := Set}}}} ->
                      {serial_in_khepri(MaybeSerial, X), Set};
                  _ ->
                      no_recover
              end
      end,
      fun (no_recover, _) -> ok;
          ({Serial, Set}, _) ->
              sets:fold(
                fun(B, _) ->
                        rabbit_exchange:callback(X, add_binding, [transaction, Serial], [X, B])
                end, none, Set),
              ok
      end,
      khepri).

list_queues_with_possible_retry_in_mnesia(Fun) ->
    %% amqqueue migration:
    %% The `rabbit_queue` or `rabbit_durable_queue` tables
    %% might be migrated between the time we query the pattern
    %% (with the `amqqueue` module) and the time we call
    %% `mnesia:dirty_match_object()`. This would lead to an empty list
    %% (no object matching the now incorrect pattern), not a Mnesia
    %% error.
    %%
    %% So if the result is an empty list and the version of the
    %% `amqqueue` record changed in between, we retry the operation.
    %%
    %% However, we don't do this if inside a Mnesia transaction: we
    %% could end up with a live lock between this started transaction
    %% and the Mnesia table migration which is blocked (but the
    %% rabbit_feature_flags lock is held).
    AmqqueueRecordVersion = amqqueue:record_version_to_use(),
    case Fun() of
        [] ->
            case mnesia:is_transaction() of
                true ->
                    [];
                false ->
                    case amqqueue:record_version_to_use() of
                        AmqqueueRecordVersion -> [];
                        _                     -> Fun()
                    end
            end;
        Ret ->
            Ret
    end.

list_queues_with_possible_retry_in_khepri(Fun) ->
    %% See equivalent `list_queues_with_possible_retry_in_mnesia` first.
    %% Not sure how much of this is possible in Khepri, as there is no dirty read,
    %% but the amqqueue record migration is still happening.
    %% Let's retry just in case
    AmqqueueRecordVersion = amqqueue:record_version_to_use(),
    case Fun() of
        [] ->
            case khepri_tx:is_transaction() of
                true ->
                    [];
                false ->
                    case amqqueue:record_version_to_use() of
                        AmqqueueRecordVersion -> [];
                        _                     -> Fun()
                    end
            end;
        Ret ->
            Ret
    end.

list_queues_for_count(VHost) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              %% this is certainly suboptimal but there is no way to count
              %% things using a secondary index in Mnesia. Our counter-table-per-node
              %% won't work here because with master migration of mirrored queues
              %% the "ownership" of queues by nodes becomes a non-trivial problem
              %% that requires a proper consensus algorithm.
              list_queues_with_possible_retry_in_mnesia(
                fun() ->
                        length(mnesia:dirty_index_read(rabbit_queue,
                                                       VHost,
                                                       amqqueue:field_vhost()))
                end)
      end,
      fun() -> list_queues_with_possible_retry_in_khepri(
                 fun() ->
                         count_in_khepri(khepri_queues_path() ++ [VHost])
                 end)
      end).

internal_delete_queue_in_mnesia(QueueName, OnlyDurable, Reason) ->
    ok = mnesia:delete({rabbit_queue, QueueName}),
    case Reason of
        auto_delete ->
            case mnesia:wread({rabbit_durable_queue, QueueName}) of
                []  -> ok;
                [_] -> ok = mnesia:delete({rabbit_durable_queue, QueueName})
            end;
        _ ->
            mnesia:delete({rabbit_durable_queue, QueueName})
    end,
    %% we want to execute some things, as decided by rabbit_exchange,
    %% after the transaction.
    remove_bindings_for_destination_in_mnesia(QueueName, OnlyDurable).

internal_delete_queue_in_khepri(QueueName, OnlyDurable, _Reason) ->
    Path = khepri_queue_path(QueueName),
    DurablePath = khepri_durable_queue_path(QueueName),
    {ok, _} = khepri_tx:delete(Path),
    {ok, _} = khepri_tx:delete(DurablePath),
    %% we want to execute some things, as decided by rabbit_exchange,
    %% after the transaction.
    remove_bindings_for_destination_in_khepri(QueueName, OnlyDurable).

delete_queue_in_mnesia(QueueName, Reason) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              case {mnesia:wread({rabbit_queue, QueueName}),
                    mnesia:wread({rabbit_durable_queue, QueueName})} of
                  {[], []} ->
                      ok;
                  _ ->
                      Deletions0 = internal_delete_queue_in_mnesia(QueueName, false, Reason),
                      Deletions = rabbit_binding:process_deletions(Deletions0, true),
                      {Deletions, false}
              end
      end).

delete_queue_in_khepri(Name, Reason) ->
    rabbit_khepri:transaction(
      fun () ->
              case {lookup_tx_in_khepri(khepri_queue_path(Name)),
                    lookup_tx_in_khepri(khepri_durable_queue_path(Name))} of
                  {[], []} ->
                      ok;
                  _ ->
                      {internal_delete_queue_in_khepri(Name, false, Reason), all}
              end
      end, rw).

delete_transient_queue_in_mnesia(QName) ->
    ok = mnesia:delete({rabbit_queue, QName}),
    remove_transient_bindings_for_destination_in_mnesia(QName).

delete_transient_queue_in_khepri(QName) ->
    {ok, _} = khepri_tx:delete(khepri_queue_path(QName)),
    remove_transient_bindings_for_destination_in_khepri(QName).

update_queue_in_mnesia(Name, Fun) ->
    case mnesia:wread({rabbit_queue, Name}) of
        [Q] ->
            Durable = amqqueue:is_durable(Q),
            Q1 = Fun(Q),
            ok = mnesia:write(rabbit_queue, Q1, write),
            case Durable of
                true -> ok = mnesia:write(rabbit_durable_queue, Q1, write);
                _    -> ok
            end,
            Q1;
        [] ->
            not_found
    end.

update_queue_in_khepri(Name, Fun) ->
    Path = khepri_queue_path(Name),
    case khepri_tx:get(Path) of
        {ok, #{Path := #{data := Q}}} ->
            Durable = amqqueue:is_durable(Q),
            Q1 = Fun(Q),
            {ok, _} = khepri_tx:put(Path, Q1),
            case Durable of
                true ->
                    DurablePath = khepri_durable_queue_path(Name),
                    {ok, _} = khepri_tx:put(DurablePath, Q1);
                _ ->
                    ok
            end,
            Q1;
        _  ->
            not_found
    end.

update_queue_decorators_in_mnesia(Name) ->
    rabbit_misc:execute_mnesia_transaction(
      fun() ->
              case mnesia:wread({rabbit_queue, Name}) of
                  [Q] -> ok = mnesia:write(rabbit_queue, rabbit_queue_decorators:set(Q), write);
                  []  -> ok
              end
      end).

update_queue_decorators_in_khepri(Name) ->
    Path = khepri_queue_path(Name),
    %% Decorators are stored on an ETS table, we need to query them before the transaction.
    %% Also, to verify which ones are active could lead to any kind of side-effects.
    %% Thus it needs to be done outside of the transaction
    Decorators = case rabbit_khepri:get(Path) of
                     {ok, #{Path := #{data := Q}}} ->
                         rabbit_queue_decorator:active(Q);
                     _ ->
                         []
                 end,
    rabbit_khepri:transaction(
      fun() ->
              case khepri_tx:get(Path) of
                  {ok, #{Path := #{data := Q0}}} ->
                      Q1 = amqqueue:reset_mirroring_and_decorators(Q0),
                      Q2 = amqqueue:set_decorators(Q1, Decorators),
                      store_in_khepri(Path, Q2),
                      ok;
                  _  ->
                      ok
              end
      end, rw).

store_in_khepri(Path, Value) ->
    case khepri_tx:put(Path, Value) of
        {ok, _} -> ok;
        Error   -> khepri_tx:abort(Error)
    end.

add_listener_in_khepri(#listener{node = Node} = Listener) ->
    rabbit_khepri:transaction(
      fun() ->
              Path = khepri_listener_path(Node),
              Set0 = case khepri_tx:get(Path) of
                         {ok, #{Path := #{data := S}}} -> S;
                         _ -> sets:new()
                     end,
              Set = sets:add_element(Listener, Set0),
              case khepri_tx:put(Path, Set) of
                  {ok, _} -> ok;
                  Error -> khepri_tx:abort(Error)
              end
      end).
