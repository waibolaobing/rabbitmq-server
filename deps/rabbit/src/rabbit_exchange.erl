%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_exchange).
-include_lib("khepri/include/khepri.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

-export([recover/1, policy_changed/2, callback/4, declare/7,
         assert_equivalence/6, assert_args_equivalence/2, check_type/1,
         lookup/1, lookup_many/1, lookup_or_die/1, list/0, list/1, lookup_scratch/2,
         update_scratch/3, update_decorators/1, immutable/1,
         info_keys/0, info/1, info/2, info_all/1, info_all/2, info_all/4,
         route/2, delete/3, validate_binding/2, count/0]).
-export([list_names/0, is_amq_prefixed/1]).
%% these must be run inside a mnesia tx
-export([serial_in_mnesia/1, serial_in_khepri/1, peek_serial/1, update/2]).
-export([maybe_auto_delete_in_mnesia/2, maybe_auto_delete_in_khepri/2]).
-export([peek_serial_in_mnesia/1, peek_serial_in_khepri/1]).
-export([mnesia_write_exchange_to_khepri/1, mnesia_write_durable_exchange_to_khepri/1,
         mnesia_write_exchange_serial_to_khepri/1, mnesia_delete_exchange_to_khepri/1,
         mnesia_delete_durable_exchange_to_khepri/1, mnesia_delete_exchange_serial_to_khepri/1,
         clear_exchange_data_in_khepri/0, clear_durable_exchange_data_in_khepri/0,
         clear_exchange_serial_data_in_khepri/0]).
-export([list_in_mnesia/2, list_in_khepri_tx/1, update_in_mnesia/2, update_in_khepri/2]).
-export([list_in_mnesia/1, list_in_khepri/1, store_in_khepri/2,
         lookup_as_list_in_khepri/1, lookup_in_khepri/2]).

%%----------------------------------------------------------------------------

-export_type([name/0, type/0]).

-type name() :: rabbit_types:r('exchange').
-type type() :: atom().
-type fun_name() :: atom().

%%----------------------------------------------------------------------------

-define(INFO_KEYS, [name, type, durable, auto_delete, internal, arguments,
                    policy, user_who_performed_action]).

-spec recover(rabbit_types:vhost()) -> [name()].

recover(VHost) ->
    Xs = rabbit_khepri:try_mnesia_or_khepri(
           fun() -> rabbit_misc:table_filter(
                      fun (#exchange{name = XName}) ->
                              XName#resource.virtual_host =:= VHost andalso
                                  mnesia:read({rabbit_exchange, XName}) =:= []
                      end,
                      fun (X, Tx) ->
                              X1 = case Tx of
                                       true  -> store_ram_in_mnesia(X);
                                       false -> rabbit_exchange_decorator:set(X)
                                   end,
                              %% TODO how do we guarantee that lower down the callback
                              %% we use the right transaction type? mnesia/khepri
                              callback(X1, create, map_create_tx(Tx), [X1])
                      end,
                      rabbit_durable_exchange)
           end,
           fun() ->
                   Exchanges = rabbit_khepri_misc:table_filter_in_khepri(
                                 fun (#exchange{name = XName}) ->
                                         XName#resource.virtual_host =:= VHost andalso
                                             lookup_as_list_in_khepri(XName) =:= []
                                 end,
                                 fun (X) ->
                                         store_ram_in_khepri(X)
                                 end,
                                 khepri_durable_exchanges_path()),
                   [begin
                        X1 = rabbit_exchange_decorator:set(X),
                        callback(X1, create, transaction, [X1]),
                        callback(X1, create, none, [X1])
                    end || X <- Exchanges],
                   Exchanges
           end),
    [XName || #exchange{name = XName} <- Xs].

-spec callback
        (rabbit_types:exchange(), fun_name(), atom(), [any()]) -> 'ok'.

callback(X = #exchange{type       = XType,
                       decorators = Decorators}, Fun, Serial0, Args) ->
    Serial = if is_function(Serial0) -> Serial0;
                is_atom(Serial0)     -> fun (_Bool) -> Serial0 end
             end,
    [ok = apply(M, Fun, [Serial(M:serialise_events(X)) | Args]) ||
        M <- rabbit_exchange_decorator:select(all, Decorators)],
    Module = type_to_module(XType),
    apply(Module, Fun, [Serial(Module:serialise_events()) | Args]).

-spec policy_changed
        (rabbit_types:exchange(), rabbit_types:exchange()) -> 'ok'.

policy_changed(X  = #exchange{type       = XType,
                              decorators = Decorators},
               X1 = #exchange{decorators = Decorators1}) ->
    D  = rabbit_exchange_decorator:select(all, Decorators),
    D1 = rabbit_exchange_decorator:select(all, Decorators1),
    DAll = lists:usort(D ++ D1),
    [ok = M:policy_changed(X, X1) || M <- [type_to_module(XType) | DAll]],
    ok.

serialise_events(X = #exchange{type = Type, decorators = Decorators}) ->
    lists:any(fun (M) -> M:serialise_events(X) end,
              rabbit_exchange_decorator:select(all, Decorators))
        orelse (type_to_module(Type)):serialise_events().

-spec serial_in_mnesia(rabbit_types:exchange()) ->
          fun((boolean()) -> 'none' | pos_integer()).
serial_in_mnesia(#exchange{name = XName} = X) ->
    Serial = case serialise_events(X) of
                 true  -> next_serial_in_mnesia(XName);
                 false -> none
             end,
    fun (true)  -> Serial;
        (false) -> none
    end.

serial_in_khepri(#exchange{name = XName} = X) ->
    Serial = case serialise_events(X) of
                 true  -> next_serial_in_khepri(XName);
                 false -> none
             end,
    fun (true)  -> Serial;
        (false) -> none
    end.

-spec is_amq_prefixed(rabbit_types:exchange() | binary()) -> boolean().

is_amq_prefixed(Name) when is_binary(Name) ->
    case re:run(Name, <<"^amq\.">>) of
        nomatch    -> false;
        {match, _} -> true
    end;
is_amq_prefixed(#exchange{name = #resource{name = <<>>}}) ->
    false;
is_amq_prefixed(#exchange{name = #resource{name = Name}}) ->
    is_amq_prefixed(Name).

-spec declare
        (name(), type(), boolean(), boolean(), boolean(),
         rabbit_framing:amqp_table(), rabbit_types:username())
        -> rabbit_types:exchange().

declare(XName, Type, Durable, AutoDelete, Internal, Args, Username) ->
    X = rabbit_exchange_decorator:set(
          rabbit_policy:set(#exchange{name        = XName,
                                      type        = Type,
                                      durable     = Durable,
                                      auto_delete = AutoDelete,
                                      internal    = Internal,
                                      arguments   = Args,
                                      options     = #{user => Username}})),
    XT = type_to_module(Type),
    %% We want to upset things if it isn't ok
    ok = XT:validate(X),
    %% Avoid a channel exception if there's a race condition
    %% with an exchange.delete operation.
    %%
    %% See rabbitmq/rabbitmq-federation#7.
    case rabbit_runtime_parameters:lookup(XName#resource.virtual_host,
                                          ?EXCHANGE_DELETE_IN_PROGRESS_COMPONENT,
                                          XName#resource.name) of
        not_found ->
            rabbit_khepri:try_mnesia_or_khepri(
              fun() ->
                      rabbit_misc:execute_mnesia_transaction(
                        fun () ->
                                case mnesia:wread({rabbit_exchange, XName}) of
                                    [] ->
                                        {new, store_in_mnesia(X)};
                                    [ExistingX] ->
                                        {existing, ExistingX}
                                end
                        end,
                        fun ({new, Exchange}, Tx) ->
                                ok = callback(X, create, map_create_tx(Tx), [Exchange]),
                                rabbit_event:notify_if(not Tx, exchange_created, info(Exchange)),
                                Exchange;
                            ({existing, Exchange}, _Tx) ->
                                Exchange;
                            (Err, _Tx) ->
                                Err
                        end)
              end,
              fun() ->
                      rabbit_khepri_misc:execute_khepri_transaction(
                        fun() ->
                                case lookup_as_list_in_khepri(XName) of
                                    [] ->
                                        {new, store_in_khepri(X)};
                                    [ExistingX] ->
                                        {existing, ExistingX}
                                end
                        end,
                        fun ({new, Exchange}) ->
                                ok = callback(X, create, transaction, [Exchange]),
                                ok = callback(X, create, none, [Exchange]),
                                rabbit_event:notify(exchange_created, info(Exchange)),
                                Exchange;
                            ({existing, Exchange}) ->
                                Exchange;
                            (Err) ->
                                Err
                        end)
              end);
        _ ->
            rabbit_log:warning("ignoring exchange.declare for exchange ~p,
                                exchange.delete in progress~n.", [XName]),
            X
    end.

map_create_tx(true)  -> transaction;
map_create_tx(false) -> none.


store_in_mnesia(X = #exchange{durable = true}) ->
    mnesia:write(rabbit_durable_exchange, X#exchange{decorators = undefined},
                 write),
    store_ram_in_mnesia(X);
store_in_mnesia(X = #exchange{durable = false}) ->
    store_ram_in_mnesia(X).

store_ram_in_mnesia(X) ->
    X1 = rabbit_exchange_decorator:set(X),
    ok = mnesia:write(rabbit_exchange, rabbit_exchange_decorator:set(X1),
                      write),
    X1.

store_in_khepri(X = #exchange{durable = true}) ->
    Path = khepri_durable_exchange_path(X#exchange.name),
    {ok, _} = khepri_tx:put(Path, #kpayload_data{data = X#exchange{decorators = undefined}}),
    store_ram_in_khepri(X);
store_in_khepri(X = #exchange{durable = false}) ->
    store_ram_in_khepri(X).

store_ram_in_khepri(X) ->
    Path = khepri_exchange_path(X#exchange.name),
    {ok, _} = khepri_tx:put(Path, #kpayload_data{data = X}),
    X.

store_in_khepri(X = #exchange{durable = true}, TableName) ->
    Path = mnesia_table_to_khepri_path(TableName, X#exchange.name),
    {ok, _} = khepri_tx:put(Path, #kpayload_data{data = X}),
    ok.

%% Used with binaries sent over the wire; the type may not exist.

-spec check_type
        (binary()) -> atom() | rabbit_types:connection_exit().

check_type(TypeBin) ->
    case rabbit_registry:binary_to_type(rabbit_data_coercion:to_binary(TypeBin)) of
        {error, not_found} ->
            rabbit_misc:protocol_error(
              command_invalid, "unknown exchange type '~s'", [TypeBin]);
        T ->
            case rabbit_registry:lookup_module(exchange, T) of
                {error, not_found} -> rabbit_misc:protocol_error(
                                        command_invalid,
                                        "invalid exchange type '~s'", [T]);
                {ok, _Module}      -> T
            end
    end.

-spec assert_equivalence
        (rabbit_types:exchange(), atom(), boolean(), boolean(), boolean(),
         rabbit_framing:amqp_table())
        -> 'ok' | rabbit_types:connection_exit().

assert_equivalence(X = #exchange{ name        = XName,
                                  durable     = Durable,
                                  auto_delete = AutoDelete,
                                  internal    = Internal,
                                  type        = Type},
                   ReqType, ReqDurable, ReqAutoDelete, ReqInternal, ReqArgs) ->
    AFE = fun rabbit_misc:assert_field_equivalence/4,
    AFE(Type,       ReqType,       XName, type),
    AFE(Durable,    ReqDurable,    XName, durable),
    AFE(AutoDelete, ReqAutoDelete, XName, auto_delete),
    AFE(Internal,   ReqInternal,   XName, internal),
    (type_to_module(Type)):assert_args_equivalence(X, ReqArgs).

-spec assert_args_equivalence
        (rabbit_types:exchange(), rabbit_framing:amqp_table())
        -> 'ok' | rabbit_types:connection_exit().

assert_args_equivalence(#exchange{ name = Name, arguments = Args },
                        RequiredArgs) ->
    %% The spec says "Arguments are compared for semantic
    %% equivalence".  The only arg we care about is
    %% "alternate-exchange".
    rabbit_misc:assert_args_equivalence(Args, RequiredArgs, Name,
                                        [<<"alternate-exchange">>]).

-spec lookup
        (name()) -> rabbit_types:ok(rabbit_types:exchange()) |
                    rabbit_types:error('not_found').

lookup(Name) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> lookup_in_mnesia(Name) end,
      fun() -> lookup_in_khepri(Name) end).

lookup_in_mnesia(Name) ->
    rabbit_misc:dirty_read({rabbit_exchange, Name}).

lookup_in_khepri(Table, Name) ->
    Path = mnesia_table_to_khepri_path(Table, Name),
    case khepri_tx:get(Path) of
        {ok, #{data := X}} -> {ok, X};
        _ -> {error, not_found}
    end.

lookup_in_khepri(Name) ->
    Path = khepri_exchange_path(Name),
    case rabbit_khepri:get_data(Path) of
        {ok, X} -> {ok, X};
        _ -> {error, not_found}
    end.

lookup_as_list_in_khepri(Name) ->
    Path = khepri_exchange_path(Name),
    case khepri_tx:get(Path) of
        {ok, #{Path := #{data := X}}} -> [X];
        _ -> []
    end.

-spec lookup_many([name()]) -> [rabbit_types:exchange()].

%% TODO this one
lookup_many([]) ->
    [];
lookup_many(Names) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> lookup_many_in_mnesia(Names) end,
      fun() -> lookup_many_in_khepri(Names) end).

lookup_many_in_mnesia([Name]) -> ets:lookup(rabbit_exchange, Name);
lookup_many_in_mnesia(Names) when is_list(Names) ->
    %% Normally we'd call mnesia:dirty_read/1 here, but that is quite
    %% expensive for reasons explained in rabbit_misc:dirty_read/1.
    lists:append([ets:lookup(rabbit_exchange, Name) || Name <- Names]).

lookup_many_in_khepri(Names) when is_list(Names) ->
    lists:foldl(fun(Name, Acc) ->
                        case lookup_in_khepri(Name) of
                            {ok, X} -> [X | Acc];
                            _ -> Acc
                        end
                end, [], Names).

-spec lookup_or_die
        (name()) -> rabbit_types:exchange() |
                    rabbit_types:channel_exit().

lookup_or_die(Name) ->
    case lookup(Name) of
        {ok, X}            -> X;
        {error, not_found} -> rabbit_amqqueue:not_found(Name)
    end.

-spec list() -> [rabbit_types:exchange()].

list() ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> list_in_mnesia(rabbit_exchange) end,
      fun() -> list_in_khepri(rabbit_exchange) end).

list_in_mnesia(Table) ->
    mnesia:dirty_match_object(Table, #exchange{_ = '_'}).

list_in_khepri(Table) ->
    Path = mnesia_table_to_khepri_path(Table),
    case rabbit_khepri:list_child_data(Path) of
        {ok, Queues} -> maps:values(Queues);
        _            -> []
    end.

-spec count() -> non_neg_integer().

count() ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> count_in_mnesia() end,
      fun() -> count_in_khepri() end).

count_in_mnesia() ->
    mnesia:table_info(rabbit_exchange, size).

count_in_khepri() ->
    Path = khepri_exchanges_path(),
    case rabbit_khepri:list_child_data(Path) of
        {ok, Xs} -> length(Xs);
        _            -> 0
    end.

-spec list_names() -> [rabbit_exchange:name()].

list_names() ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> list_names_in_mnesia() end,
      fun() -> list_names_in_khepri() end).

list_names_in_mnesia() ->
    mnesia:dirty_all_keys(rabbit_exchange).

list_names_in_khepri() ->
    Path = khepri_exchanges_path(),
    case rabbit_khepri:list_child_nodes(Path) of
        {ok, Result} -> Result;
        _            -> []
    end.

%% Not dirty_match_object since that would not be transactional when used in a
%% tx context

-spec list(rabbit_types:vhost()) -> [rabbit_types:exchange()].

list(VHostPath) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              list_in_mnesia(rabbit_exchange, VHostPath)
      end,
      fun() ->
              list_in_khepri(rabbit_exchange, VHostPath)
      end).

list_in_mnesia(Table, VHostPath) ->
    mnesia:async_dirty(
      fun () ->
              mnesia:match_object(
                Table,
                #exchange{name = rabbit_misc:r(VHostPath, exchange), _ = '_'},
                read)
      end).

list_in_khepri(Table, VHostPath) ->
    Path = mnesia_table_to_khepri_path(Table) ++ [VHostPath, ?STAR_STAR],
    {ok, Map} = rabbit_khepri:match_and_get_data(Path),
    maps:fold(fun(_, X, Acc) -> [X | Acc] end, [], Map).

list_in_khepri_tx(VHostPath) ->
    Path = khepri_exchanges_path() ++ [VHostPath, ?STAR_STAR],
    {ok, Map} = rabbit_khepri:tx_match_and_get_data(Path),
    maps:fold(fun(_, X, Acc) -> [X | Acc] end, [], Map).

-spec lookup_scratch(name(), atom()) ->
                               rabbit_types:ok(term()) |
                               rabbit_types:error('not_found').

lookup_scratch(Name, App) ->
    case lookup(Name) of
        {ok, #exchange{scratches = undefined}} ->
            {error, not_found};
        {ok, #exchange{scratches = Scratches}} ->
            case orddict:find(App, Scratches) of
                {ok, Value} -> {ok, Value};
                error       -> {error, not_found}
            end;
        {error, not_found} ->
            {error, not_found}
    end.

-spec update_scratch(name(), atom(), fun((any()) -> any())) -> 'ok'.

update_scratch(Name, App, Fun) ->
    Decorators = rabbit_exchange_decorator:list(),
    rabbit_khepri:try_mnesia_or_khepri(
      fun () ->
              rabbit_misc:execute_mnesia_transaction(
                fun() ->
                        update_in_mnesia(Name, update_scratch(App, Fun, [])),
                        ok
                end)
      end,
      fun() ->
              Path = khepri_exchange_path(Name),
              Decorators = case rabbit_khepri:get(Path) of
                               {ok, #{Path := #{data := X}}} ->
                                   rabbit_exchange_decorator:active(X);
                               _ ->
                                   []
                           end,
              rabbit_khepri:transaction(
                fun() ->
                        update_in_khepri(Name, update_scratch_fun(App, Fun, Decorators)),
                        ok
                end)
      end).

update_scratch_fun(App, Fun, Decorators) ->
    fun(X = #exchange{scratches = Scratches0}) ->
            Scratches1 = case Scratches0 of
                             undefined -> orddict:new();
                             _         -> Scratches0
                         end,
            Scratch = case orddict:find(App, Scratches1) of
                          {ok, S} -> S;
                          error   -> undefined
                      end,
            Scratches2 = orddict:store(App, Fun(Scratch), Scratches1),
            X#exchange{scratches = Scratches2,
                       decorators = Decorators}
    end.

-spec update_decorators(name()) -> 'ok'.

update_decorators(Name) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> update_decorators_in_mnesia(Name) end,
      fun() -> update_decorators_in_khepri(Name) end).

update_decorators_in_mnesia(Name) ->
    rabbit_misc:execute_mnesia_transaction(
      fun() ->
              case mnesia:wread({rabbit_exchange, Name}) of
                  [X] -> store_ram_in_mnesia(X),
                         ok;
                  []  -> ok
              end
      end).

update_decorators_in_khepri(Name) ->
    rabbit_khepri:transaction(
      fun() ->
              case lookup_as_list_in_khepri(Name) of
                  [X] -> store_ram_in_khepri(X),
                         ok;
                  []  -> ok
              end
      end).

-spec update
        (name(),
         fun((rabbit_types:exchange()) -> rabbit_types:exchange()))
         -> not_found | rabbit_types:exchange().

update(Name, Fun) ->
    %% TODO is this called from anywhere else? The double check of `rabbit_khepri:try...`
    %% will have issues when compiling the transaction
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> update_in_mnesia(Name, Fun) end,
      fun() -> update_in_khepri(Name, Fun) end).

update_in_mnesia(Name, Fun) ->
    case mnesia:wread({rabbit_exchange, Name}) of
        [X] -> X1 = Fun(X),
               store_in_mnesia(X1);
        []  -> not_found
    end.

update_in_khepri(Name, Fun) ->
    case lookup_as_list_in_khepri(Name) of
        [X] -> X1 = Fun(X),
               store_in_khepri(X1);
        []  -> not_found
    end.

-spec immutable(rabbit_types:exchange()) -> rabbit_types:exchange().

immutable(X) -> X#exchange{scratches  = none,
                           policy     = none,
                           decorators = none}.

-spec info_keys() -> rabbit_types:info_keys().

info_keys() -> ?INFO_KEYS.

map(VHostPath, F) ->
    %% TODO: there is scope for optimisation here, e.g. using a
    %% cursor, parallelising the function invocation
    lists:map(F, list(VHostPath)).

infos(Items, X) -> [{Item, i(Item, X)} || Item <- Items].

i(name,        #exchange{name        = Name})       -> Name;
i(type,        #exchange{type        = Type})       -> Type;
i(durable,     #exchange{durable     = Durable})    -> Durable;
i(auto_delete, #exchange{auto_delete = AutoDelete}) -> AutoDelete;
i(internal,    #exchange{internal    = Internal})   -> Internal;
i(arguments,   #exchange{arguments   = Arguments})  -> Arguments;
i(policy,      X) ->  case rabbit_policy:name(X) of
                          none   -> '';
                          Policy -> Policy
                      end;
i(user_who_performed_action, #exchange{options = Opts}) ->
    maps:get(user, Opts, ?UNKNOWN_USER);
i(Item, #exchange{type = Type} = X) ->
    case (type_to_module(Type)):info(X, [Item]) of
        [{Item, I}] -> I;
        []          -> throw({bad_argument, Item})
    end.

-spec info(rabbit_types:exchange()) -> rabbit_types:infos().

info(X = #exchange{type = Type}) ->
    infos(?INFO_KEYS, X) ++ (type_to_module(Type)):info(X).

-spec info
        (rabbit_types:exchange(), rabbit_types:info_keys())
        -> rabbit_types:infos().

info(X = #exchange{type = _Type}, Items) ->
    infos(Items, X).

-spec info_all(rabbit_types:vhost()) -> [rabbit_types:infos()].

info_all(VHostPath) -> map(VHostPath, fun (X) -> info(X) end).

-spec info_all(rabbit_types:vhost(), rabbit_types:info_keys())
                   -> [rabbit_types:infos()].

info_all(VHostPath, Items) -> map(VHostPath, fun (X) -> info(X, Items) end).

-spec info_all(rabbit_types:vhost(), rabbit_types:info_keys(),
                    reference(), pid())
                   -> 'ok'.

info_all(VHostPath, Items, Ref, AggregatorPid) ->
    rabbit_control_misc:emitting_map(
      AggregatorPid, Ref, fun(X) -> info(X, Items) end, list(VHostPath)).

-spec route(rabbit_types:exchange(), rabbit_types:delivery())
                 -> [rabbit_amqqueue:name()].

route(#exchange{name = #resource{virtual_host = VHost, name = RName} = XName,
                decorators = Decorators} = X,
      #delivery{message = #basic_message{routing_keys = RKs}} = Delivery) ->
    case RName of
        <<>> ->
            RKsSorted = lists:usort(RKs),
            [rabbit_channel:deliver_reply(RK, Delivery) ||
                RK <- RKsSorted, virtual_reply_queue(RK)],
            [rabbit_misc:r(VHost, queue, RK) || RK <- RKsSorted,
                                                not virtual_reply_queue(RK)];
        _ ->
            Decs = rabbit_exchange_decorator:select(route, Decorators),
            lists:usort(route1(Delivery, Decs, {[X], XName, []}))
    end.

virtual_reply_queue(<<"amq.rabbitmq.reply-to.", _/binary>>) -> true;
virtual_reply_queue(_)                                      -> false.

route1(_, _, {[], _, QNames}) ->
    QNames;
route1(Delivery, Decorators,
       {[X = #exchange{type = Type} | WorkList], SeenXs, QNames}) ->
    ExchangeDests  = (type_to_module(Type)):route(X, Delivery),
    DecorateDests  = process_decorators(X, Decorators, Delivery),
    AlternateDests = process_alternate(X, ExchangeDests),
    route1(Delivery, Decorators,
           lists:foldl(fun process_route/2, {WorkList, SeenXs, QNames},
                       AlternateDests ++ DecorateDests  ++ ExchangeDests)).

process_alternate(X = #exchange{name = XName}, []) ->
    case rabbit_policy:get_arg(
           <<"alternate-exchange">>, <<"alternate-exchange">>, X) of
        undefined -> [];
        AName     -> [rabbit_misc:r(XName, exchange, AName)]
    end;
process_alternate(_X, _Results) ->
    [].

process_decorators(_, [], _) -> %% optimisation
    [];
process_decorators(X, Decorators, Delivery) ->
    lists:append([Decorator:route(X, Delivery) || Decorator <- Decorators]).

process_route(#resource{kind = exchange} = XName,
              {_WorkList, XName, _QNames} = Acc) ->
    Acc;
process_route(#resource{kind = exchange} = XName,
              {WorkList, #resource{kind = exchange} = SeenX, QNames}) ->
    {cons_if_present(XName, WorkList),
     gb_sets:from_list([SeenX, XName]), QNames};
process_route(#resource{kind = exchange} = XName,
              {WorkList, SeenXs, QNames} = Acc) ->
    case gb_sets:is_element(XName, SeenXs) of
        true  -> Acc;
        false -> {cons_if_present(XName, WorkList),
                  gb_sets:add_element(XName, SeenXs), QNames}
    end;
process_route(#resource{kind = queue} = QName,
              {WorkList, SeenXs, QNames}) ->
    {WorkList, SeenXs, [QName | QNames]}.

cons_if_present(XName, L) ->
    case lookup(XName) of
        {ok, X}            -> [X | L];
        {error, not_found} -> L
    end.

call_with_exchange_in_mnesia(XName, Fun) ->
    rabbit_misc:execute_mnesia_tx_with_tail(
      fun () -> case mnesia:read({rabbit_exchange, XName}) of
                    []  -> rabbit_misc:const({error, not_found});
                    [X] -> Fun(X)
                end
      end).

-spec delete
        (name(),  'true', rabbit_types:username()) ->
                    'ok'| rabbit_types:error('not_found' | 'in_use');
        (name(), 'false', rabbit_types:username()) ->
                    'ok' | rabbit_types:error('not_found').

delete(XName, IfUnused, Username) ->
    MnesiaFun = case IfUnused of
                    true  -> fun conditional_delete_in_mnesia/2;
                    false -> fun unconditional_delete_in_mnesia/2
                end,
    KhepriFun = case IfUnused of
                    true  -> fun conditional_delete_in_khepri/2;
                    false -> fun unconditional_delete_in_khepri/2
                end,
    try
        %% guard exchange.declare operations from failing when there's
        %% a race condition between it and an exchange.delete.
        %%
        %% see rabbitmq/rabbitmq-federation#7
        rabbit_runtime_parameters:set(XName#resource.virtual_host,
                                      ?EXCHANGE_DELETE_IN_PROGRESS_COMPONENT,
                                      XName#resource.name, true, Username),
        rabbit_khepri:try_mnesia_or_khepri(
          fun() ->
                  call_with_exchange_in_mnesia(
                    XName,
                    fun (X) ->
                            case MnesiaFun(X, false) of
                                {deleted, X, Bs, Deletions} ->
                                    rabbit_binding:process_deletions(
                                      rabbit_binding:add_deletion(
                                        XName, {X, deleted, Bs}, Deletions), Username);
                                {error, _InUseOrNotFound} = E ->
                                    rabbit_misc:const(E)
                            end
                    end)
          end,
          fun() ->
                  case rabbit_khepri:transaction(
                         fun() ->
                                 case lookup_as_list_in_khepri(XName) of
                                     [] -> {error, not_found};
                                     [X] -> KhepriFun(X, false)
                                 end
                         end) of
                      {error, _} = E ->
                          E;
                      {deleted, X, Bs, Deletions} ->
                          %% TODO this bindings might notify things? review!
                          %% TODO process_deletions should happen outside the transaction
                          rabbit_binding:process_deletions_in_khepri(
                            rabbit_binding:add_deletion(
                              XName, {X, deleted, Bs}, Deletions), Username)
                  end
          end)
    after
        rabbit_runtime_parameters:clear(XName#resource.virtual_host,
                                        ?EXCHANGE_DELETE_IN_PROGRESS_COMPONENT,
                                        XName#resource.name, Username)
    end.

-spec validate_binding
        (rabbit_types:exchange(), rabbit_types:binding())
        -> rabbit_types:ok_or_error({'binding_invalid', string(), [any()]}).

validate_binding(X = #exchange{type = XType}, Binding) ->
    Module = type_to_module(XType),
    Module:validate_binding(X, Binding).

-spec maybe_auto_delete_in_mnesia
        (rabbit_types:exchange(), boolean())
        -> 'not_deleted' | {'deleted', rabbit_binding:deletions()}.

maybe_auto_delete_in_mnesia(#exchange{auto_delete = false}, _OnlyDurable) ->
    not_deleted;
maybe_auto_delete_in_mnesia(#exchange{auto_delete = true} = X, OnlyDurable) ->
    case conditional_delete_in_mnesia(X, OnlyDurable) of
        {error, in_use}             -> not_deleted;
        {deleted, X, [], Deletions} -> {deleted, Deletions}
    end.

maybe_auto_delete_in_khepri(#exchange{auto_delete = false}, _OnlyDurable) ->
    not_deleted;
maybe_auto_delete_in_khepri(#exchange{auto_delete = true} = X, OnlyDurable) ->
    case conditional_delete_in_khepri(X, OnlyDurable) of
        {error, in_use}             -> not_deleted;
        {deleted, X, [], Deletions} -> {deleted, Deletions}
    end.

conditional_delete_in_mnesia(X = #exchange{name = XName}, OnlyDurable) ->
    case rabbit_binding:has_for_source_in_mnesia(XName) of
        false  -> internal_delete_in_mnesia(X, OnlyDurable, false);
        true   -> {error, in_use}
    end.

conditional_delete_in_khepri(X = #exchange{name = XName}, OnlyDurable) ->
    case rabbit_binding:has_for_source_in_khepri(XName) of
        false  -> internal_delete_in_khepri(X, OnlyDurable, false);
        true   -> {error, in_use}
    end.

unconditional_delete_in_mnesia(X, OnlyDurable) ->
    internal_delete_in_mnesia(X, OnlyDurable, true).

unconditional_delete_in_khepri(X, OnlyDurable) ->
    internal_delete_in_khepri(X, OnlyDurable, true).

internal_delete_in_mnesia(X = #exchange{name = XName}, OnlyDurable, RemoveBindingsForSource) ->
    ok = mnesia:delete({rabbit_exchange, XName}),
    ok = mnesia:delete({rabbit_exchange_serial, XName}),
    mnesia:delete({rabbit_durable_exchange, XName}),
    Bindings = case RemoveBindingsForSource of
        true  -> rabbit_binding:remove_for_source_in_mnesia(XName);
        false -> []
    end,
    {deleted, X, Bindings, rabbit_binding:remove_for_destination_in_mnesia(
                             XName, OnlyDurable)}.

internal_delete_in_khepri(X = #exchange{name = XName}, OnlyDurable, RemoveBindingsForSource) ->
    {ok, _} = khepri_tx:delete(khepri_exchange_path(XName)),
    {ok, _} = khepri_tx:delete(khepri_exchange_serial_path(XName)),
    {ok, _} = khepri_tx:delete(khepri_durable_exchange_path(XName)),
    %% TODO bindings...
    Bindings = case RemoveBindingsForSource of
        true  -> rabbit_binding:remove_for_source_in_khepri(XName);
        false -> []
    end,
    {deleted, X, Bindings, rabbit_binding:remove_for_destination_in_khepri(
                             XName, OnlyDurable)}.

next_serial_in_mnesia(XName) ->
    Serial = peek_serial_in_mnesia(XName, write),
    ok = mnesia:write(rabbit_exchange_serial,
                      #exchange_serial{name = XName, next = Serial + 1}, write),
    Serial.

next_serial_in_khepri(XName) ->
    rabbit_khepri:transaction(
      fun() ->
              Serial = peek_serial_in_khepri(XName, write),
              Path = khepri_exchange_serial_path(XName),
              {ok, _} = khepri_tx:put(Path,
                                      #exchange_serial{name = XName, next = Serial + 1}, write),
              Serial
      end).

-spec peek_serial(name()) -> pos_integer() | 'undefined'.

peek_serial(XName) -> peek_serial_in_mnesia(XName).

peek_serial_in_mnesia(XName) -> peek_serial_in_mnesia(XName, read).
peek_serial_in_khepri(XName) -> peek_serial_in_khepri(XName, read).

peek_serial_in_mnesia(XName, LockType) ->
    case mnesia:read(rabbit_exchange_serial, XName, LockType) of
        [#exchange_serial{next = Serial}]  -> Serial;
        _                                  -> 1
    end.

peek_serial_in_khepri(XName, _LockType) ->
    Path = khepri_exchange_serial_path(XName),
    case khepri_tx:get(Path) of
        {ok, #{Path := #{data := #exchange_serial{next = Serial}}}} ->
            Serial;
        _ ->
            1
    end.

invalid_module(T) ->
    rabbit_log:warning("Could not find exchange type ~s.", [T]),
    put({xtype_to_module, T}, rabbit_exchange_type_invalid),
    rabbit_exchange_type_invalid.

%% Used with atoms from records; e.g., the type is expected to exist.
type_to_module(T) ->
    case get({xtype_to_module, T}) of
        undefined ->
            case rabbit_registry:lookup_module(exchange, T) of
                {ok, Module}       -> put({xtype_to_module, T}, Module),
                                      Module;
                {error, not_found} -> invalid_module(T)
            end;
        Module ->
            Module
    end.

mnesia_table_to_khepri_path(rabbit_exchange) ->
    khepri_exchanges_path();
mnesia_table_to_khepri_path(rabbit_durable_exchange) ->
    khepri_durable_exchanges_path().

mnesia_table_to_khepri_path(rabbit_exchange, Name) ->
    khepri_exchange_path(Name);
mnesia_table_to_khepri_path(rabbit_durable_exchange, Name) ->
    khepri_durable_exchange_path(Name).

khepri_exchanges_path() ->
    [?MODULE, exchanges].

khepri_exchange_path(#resource{virtual_host = VHost, name = Name}) ->
    [?MODULE, exchanges, VHost, Name].

khepri_durable_exchanges_path() ->
    [?MODULE, durable_exchanges].

khepri_durable_exchange_path(#resource{virtual_host = VHost, name = Name}) ->
    [?MODULE, durable_exchanges, VHost, Name].

khepri_exchange_serials_path() ->
    [?MODULE, exchanges_serials].

khepri_exchange_serial_path(#resource{virtual_host = VHost, name = Name}) ->
    [?MODULE, exchange_serials, VHost, Name].

mnesia_write_exchange_to_khepri(
  #exchange{name = Resource} = Exchange) ->
    Path = khepri_exchange_path(Resource),
    case rabbit_khepri:insert(Path, Exchange) of
        ok    -> ok;
        Error -> throw(Error)
    end.

mnesia_write_durable_exchange_to_khepri(
  #exchange{name = Resource} = Exchange) ->
    Path = khepri_durable_exchange_path(Resource),
    case rabbit_khepri:insert(Path, Exchange) of
        ok    -> ok;
        Error -> throw(Error)
    end.

mnesia_write_exchange_serial_to_khepri(
  #exchange{name = Resource} = Exchange) ->
    Path = khepri_exchange_serial_path(Resource),
    case rabbit_khepri:insert(Path, Exchange) of
        ok    -> ok;
        Error -> throw(Error)
    end.

mnesia_delete_exchange_to_khepri(#exchange{name = Resource}) ->
    Path = khepri_exchange_path(Resource),
    case rabbit_khepri:delete(Path) of
        ok    -> ok;
        Error -> throw(Error)
    end.

mnesia_delete_durable_exchange_to_khepri(#exchange{name = Resource}) ->
    Path = khepri_durable_exchange_path(Resource),
    case rabbit_khepri:delete(Path) of
        ok    -> ok;
        Error -> throw(Error)
    end.

mnesia_delete_exchange_serial_to_khepri(#exchange{name = Resource}) ->
    Path = khepri_exchange_serial_path(Resource),
    case rabbit_khepri:delete(Path) of
        ok    -> ok;
        Error -> throw(Error)
    end.

clear_exchange_data_in_khepri() ->
    Path = khepri_exchanges_path(),
    case rabbit_khepri:delete(Path) of
        ok    -> ok;
        Error -> throw(Error)
    end.

clear_durable_exchange_data_in_khepri() ->
    Path = khepri_durable_exchanges_path(),
    case rabbit_khepri:delete(Path) of
        ok    -> ok;
        Error -> throw(Error)
    end.

clear_exchange_serial_data_in_khepri() ->
    Path = khepri_exchange_serials_path(),
    case rabbit_khepri:delete(Path) of
        ok    -> ok;
        Error -> throw(Error)
    end.
