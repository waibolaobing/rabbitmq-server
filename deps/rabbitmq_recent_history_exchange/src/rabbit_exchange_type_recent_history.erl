%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates. All rights reserved.

-module(rabbit_exchange_type_recent_history).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include("rabbit_recent_history.hrl").

-behaviour(rabbit_exchange_type).

-import(rabbit_misc, [table_lookup/2]).

-export([description/0, serialise_events/0, route/2]).
-export([validate/1, validate_binding/2, create/2, delete/3, add_binding/3,
         remove_bindings/3, assert_args_equivalence/2, policy_changed/2]).
-export([setup_schema/0, disable_plugin/0]).
-export([info/1, info/2]).

-rabbit_boot_step({?MODULE,
                   [{description, "exchange type x-recent-history"},
                    {mfa, {rabbit_registry, register,
                           [exchange, <<"x-recent-history">>, ?MODULE]}},
                    {cleanup, {?MODULE, disable_plugin, []}},
                    {requires, rabbit_registry},
                    {enables, kernel_ready}]}).

-rabbit_boot_step({rabbit_exchange_type_recent_history_metadata_store,
                   [{description, "recent history exchange type: metadata store"},
                    {mfa, {?MODULE, setup_schema, []}},
                    {requires, database},
                    {enables, external_infrastructure}]}).

-define(INTEGER_ARG_TYPES, [byte, short, signedint, long]).

info(_X) -> [].
info(_X, _) -> [].

description() ->
    [{name, <<"x-recent-history">>},
     {description, <<"Recent History Exchange.">>}].

serialise_events() -> false.

route(#exchange{name      = XName,
                arguments = Args},
      #delivery{message = Message}) ->
    Length = table_lookup(Args, <<"x-recent-history-length">>),
    maybe_cache_msg(XName, Message, Length),
    rabbit_router:match_routing_key(XName, ['_']).

validate(#exchange{arguments = Args}) ->
    case table_lookup(Args, <<"x-recent-history-length">>) of
        undefined   ->
            ok;
        {Type, Val} ->
            case check_int_arg(Type) of
                ok when Val > 0 ->
                    ok;
                _ ->
                    rabbit_misc:protocol_error(precondition_failed,
                                               "Invalid argument ~p, "
                                               "'x-recent-history-length' "
                                               "must be a positive integer",
                                               [Val])
            end
    end.

validate_binding(_X, _B) -> ok.
create(_Tx, _X) -> ok.
policy_changed(_X1, _X2) -> ok.

delete(transaction, #exchange{ name = XName }, _Bs) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              rabbit_misc:execute_mnesia_transaction(
                fun() ->
                        mnesia:delete(?RH_TABLE, XName, write)
                end)
      end,
      fun() ->
              rabbit_khepri:delete(khepri_recent_history_path(XName))
      end),
    ok;
delete(none, _Exchange, _Bs) ->
    ok.

add_binding(transaction, #exchange{ name = XName },
            #binding{ destination = #resource{kind = queue} = QName }) ->
    case rabbit_amqqueue:lookup(QName) of
        {error, not_found} ->
            destination_not_found_error(QName);
        {ok, Q} ->
            Msgs = get_msgs_from_cache(XName),
            deliver_messages([Q], Msgs)
    end,
    ok;
add_binding(transaction, #exchange{ name = XName },
            #binding{ destination = #resource{kind = exchange} = DestName }) ->
    case rabbit_exchange:lookup(DestName) of
        {error, not_found} ->
            destination_not_found_error(DestName);
        {ok, X} ->
            Msgs = get_msgs_from_cache(XName),
            [begin
                 Delivery = rabbit_basic:delivery(false, false, Msg, undefined),
                 Qs = rabbit_exchange:route(X, Delivery),
                 case rabbit_amqqueue:lookup(Qs) of
                     [] ->
                         destination_not_found_error(Qs);
                     QPids ->
                         deliver_messages(QPids, [Msg])
                 end
             end || Msg <- Msgs]
    end,
    ok;
add_binding(none, _Exchange, _Binding) ->
    ok.

remove_bindings(_Tx, _X, _Bs) -> ok.

assert_args_equivalence(X, Args) ->
    rabbit_exchange:assert_args_equivalence(X, Args).

%%----------------------------------------------------------------------------

setup_schema() ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              mnesia:create_table(?RH_TABLE,
                                  [{attributes, record_info(fields, cached)},
                                   {record_name, cached},
                                   {type, set}]),
              mnesia:add_table_copy(?RH_TABLE, node(), ram_copies),
              rabbit_table:wait([?RH_TABLE]),
              ok
      end,
      fun() ->
              ok
      end).

disable_plugin() ->
    rabbit_registry:unregister(exchange, <<"x-recent-history">>),
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              mnesia:delete_table(?RH_TABLE)
      end,
      fun() ->
              rabbit_khepri:delete(khepri_recent_history_path())
      end),
    ok.

%%----------------------------------------------------------------------------
%%private
maybe_cache_msg(XName,
                #basic_message{content =
                               #content{properties =
                                        #'P_basic'{headers = Headers}}}
                = Message,
                Length) ->
    case Headers of
        undefined ->
            cache_msg(XName, Message, Length);
        _ ->
            Store = table_lookup(Headers, <<"x-recent-history-no-store">>),
            case Store of
                {bool, true} ->
                    ok;
                _ ->
                    cache_msg(XName, Message, Length)
            end
    end.

cache_msg(XName, Message, Length) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              rabbit_misc:execute_mnesia_transaction(
                fun () ->
                        Cached = get_msgs_from_cache_in_mnesia(XName),
                        store_msg_in_mnesia(XName, Cached, Message, Length)
                end)
      end,
      fun() ->
              rabbit_khepri:transaction(
                fun() ->
                        Cached = get_msgs_from_cache_in_khepri_tx(XName),
                        store_msg_in_khepri_tx(XName, Cached, Message, Length)
                end)
      end).

get_msgs_from_cache(XName) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              rabbit_misc:execute_mnesia_transaction(
                fun() -> get_msgs_from_cache_in_mnesia(XName) end)
      end,
      fun() ->
              rabbit_khepri:transaction(
                fun() -> get_msgs_from_cache_in_khepri_tx(XName) end)
      end).

get_msgs_from_cache_in_mnesia(XName) ->
    case mnesia:read(?RH_TABLE, XName) of
        [] ->
            [];
        [#cached{key = XName, content=Cached}] ->
            Cached
    end.

get_msgs_from_cache_in_khepri_tx(XName) ->
    Path = khepri_recent_history_path(XName),
    case khepri_tx:get(Path) of
        {ok, #{Path := #{data := Cached}}} ->
            Cached;
        _ ->
            []
    end.

store_msg_in_mnesia(Key, Cached, Message, undefined) ->
    store_msg0_in_mnesia(Key, Cached, Message, ?KEEP_NB);
store_msg_in_mnesia(Key, Cached, Message, {_Type, Length}) ->
    store_msg0_in_mnesia(Key, Cached, Message, Length).

store_msg0_in_mnesia(Key, Cached, Message, Length) ->
    mnesia:write(?RH_TABLE,
                 #cached{key     = Key,
                         content = [Message|lists:sublist(Cached, Length-1)]},
                 write).

store_msg_in_khepri_tx(Key, Cached, Message, undefined) ->
    store_msg0_in_khepri_tx(Key, Cached, Message, ?KEEP_NB);
store_msg_in_khepri_tx(Key, Cached, Message, {_Type, Length}) ->
    store_msg0_in_khepri_tx(Key, Cached, Message, Length).

store_msg0_in_khepri_tx(Key, Cached, Message, Length) ->
    khepri_tx:put(khepri_recent_history_path(Key),
                  [Message|lists:sublist(Cached, Length-1)]).

deliver_messages(Qs, Msgs) ->
    lists:map(
      fun (Msg) ->
              Delivery = rabbit_basic:delivery(false, false, Msg, undefined),
              rabbit_amqqueue:deliver(Qs, Delivery)
      end, lists:reverse(Msgs)).

destination_not_found_error(DestName) ->
    rabbit_misc:protocol_error(
      internal_error,
      "could not find queue/exchange '~s'",
      [DestName]).

%% adapted from rabbit_amqqueue.erl
check_int_arg(Type) ->
    case lists:member(Type, ?INTEGER_ARG_TYPES) of
        true  -> ok;
        false -> {error, {unacceptable_type, Type}}
    end.

khepri_recent_history_path() ->
    [?MODULE, recent_history_exchange].

khepri_recent_history_path(#resource{virtual_host = VHost, name = Name}) ->
    [?MODULE, recent_history_exchange, VHost, Name].
