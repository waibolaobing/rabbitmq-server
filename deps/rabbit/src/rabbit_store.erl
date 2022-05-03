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

-export([list_exchanges/0, count_exchanges/0, list_exchange_names/0,
         update_exchange/3, update_exchange_decorators/1, update_exchange_scratch/2,
         create_exchange/2, list_exchanges/1, list_durable_exchanges/0,
         lookup_exchange/1, lookup_many_exchanges/1, peek_exchange_serial/3,
         next_exchange_serial/2, delete_exchange_in_khepri/3,
         delete_exchange_in_mnesia/3, delete_exchange/3,
         recover_exchanges/1]).

-export([mnesia_write_exchange_to_khepri/1, mnesia_write_durable_exchange_to_khepri/1,
         mnesia_write_exchange_serial_to_khepri/1,
         clear_exchange_data_in_khepri/0, clear_durable_exchange_data_in_khepri/0,
         clear_exchange_serial_data_in_khepri/0]).

%% TODO to become internal
-export([lookup_exchange_tx/2, list_exchanges_in_khepri_tx/1]).

%% Paths
%% --------------------------------------------------------------
khepri_exchanges_path() ->
    [?MODULE, exchanges].

khepri_exchange_path(#resource{virtual_host = VHost, name = Name}) ->
    [?MODULE, exchanges, VHost, Name].

khepri_exchange_serials_path() ->
    [?MODULE, exchanges_serials].

khepri_exchange_serial_path(#resource{virtual_host = VHost, name = Name}) ->
    [?MODULE, exchange_serials, VHost, Name].


%% API
%% --------------------------------------------------------------
create_exchange(#exchange{name = XName} = X, PrePostCommitFun) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              execute_transaction(
                fun() -> create_exchange({rabbit_exchange, XName}, X, mnesia) end,
                PrePostCommitFun,
                mnesia)
      end,
      fun() ->
              execute_transaction(
                fun() -> create_exchange(khepri_exchange_path(XName), X, khepri) end,
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
              Match = #exchange{name = rabbit_misc:r(VHost, exchange), _ = '_'},
              list_in_mnesia(rabbit_exchange, Match)
      end,
      fun() ->
              list_in_khepri(khepri_exchanges_path() ++ [VHost, ?STAR_STAR])
      end).

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

%% TODO once bindings are moved here, this should be internal
lookup_exchange_tx(Name, khepri) ->
    lookup_tx_in_khepri(khepri_exchange_path(Name)).

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

next_exchange_serial(#exchange{name = XName}, mnesia) ->
    Serial = peek_exchange_serial(XName, write, mnesia),
    ok = mnesia:write(rabbit_exchange_serial,
                      #exchange_serial{name = XName, next = Serial + 1}, write),
    Serial;
next_exchange_serial(#exchange{name = XName}, khepri) ->
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

update_exchange(Name, Fun, mnesia) ->
    update_exchange_int({rabbit_exchange, Name}, Fun, mnesia);
update_exchange(Name, Fun, khepri) ->
    update_exchange_int(khepri_exchange_path(Name), Fun, khepri).

update_exchange_decorators(Name) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> update_exchange_decorators(Name, mnesia) end,
      fun() -> update_exchange_decorators(Name, khepri) end).

update_exchange_scratch(Name, ScratchFun) ->
    rabbit_khepri:try_mnesia_or_khepri(
      update_exchange_fun(Name, ScratchFun, mnesia),
      update_exchange_fun(Name, ScratchFun, khepri)).

delete_exchange_in_mnesia(X = #exchange{name = XName}, OnlyDurable, RemoveBindingsForSource) ->
    ok = mnesia:delete({rabbit_exchange, XName}),
    mnesia:delete({rabbit_durable_exchange, XName}),
    remove_bindings_in_mnesia(X, OnlyDurable, RemoveBindingsForSource).

delete_exchange_in_khepri(X = #exchange{name = XName}, OnlyDurable, RemoveBindingsForSource) ->
    {ok, _} = khepri_tx:delete(khepri_exchange_path(XName)),
    remove_bindings_in_khepri(X, OnlyDurable, RemoveBindingsForSource).

delete_exchange(XName, DeletionFun, PrePostCommitFun) ->
    rabbit_khepri:try_mnesia_or_khepri(
      delete_exchange_fun({rabbit_exchange, XName}, DeletionFun, PrePostCommitFun, mnesia),
      delete_exchange_fun(khepri_exchange_path(XName), DeletionFun, PrePostCommitFun, khepri)).

recover_exchanges(VHost) ->
   rabbit_khepri:try_mnesia_or_khepri(
     fun() -> recover_exchanges(VHost, mnesia) end,
     fun() -> recover_exchanges(VHost, khepri) end).
             
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

lookup_tx(Name, mnesia) ->
    lookup_tx_in_mnesia(Name);
lookup_tx(Path, khepri) ->
    lookup_tx_in_khepri(Path).

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

update_exchange_int(Path, Fun, Store) ->
    case lookup_tx(Path, Store) of
        [X] -> X1 = Fun(X),
                   store_exchange(X1, Store);
        [] -> not_found
    end.

store_exchange(X = #exchange{durable = true}, mnesia) ->
    mnesia:write(rabbit_durable_exchange, X#exchange{decorators = undefined},
                 write),
    store_ram_exchange(X);
store_exchange(X = #exchange{durable = false}, mnesia) ->
    store_ram_exchange(X);
store_exchange(X, khepri) ->
    Path = khepri_exchange_path(X#exchange.name),
    {ok, _} = khepri_tx:put(Path, X),
    X.

store_ram_exchange(X) ->
    X1 = rabbit_exchange_decorator:set(X),
    ok = mnesia:write(rabbit_exchange, X1, write),
    X1.

create_exchange(NameOrPath, X, Store) ->
    case lookup(NameOrPath, Store) of
        {error, not_found} ->
            {new, store_exchange(X, Store)};
        {ok, ExistingX} ->
            {existing, ExistingX}
    end.

update_exchange_fun(Name, ScratchFun, Store) ->
    fun() ->
            execute_transaction(fun() -> update_exchange(Name, ScratchFun, Store) end, Store)
    end.              

execute_transaction(TxFun, mnesia) ->
    rabbit_misc:execute_mnesia_transaction(TxFun);
execute_transaction(TxFun, khepri) ->
    rabbit_khepri:transaction(TxFun, rw).

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

delete_exchange_fun(Name, DeletionFun, PrePostCommitFun, mnesia) ->
    fun() ->
            Fun = DeletionFun(mnesia),
            execute_transaction(fun() ->
                                        case lookup_tx_in_mnesia(Name) of
                                            [X] -> Fun(X, false);
                                            [] -> {error, not_found}
                                        end
                                end, PrePostCommitFun, mnesia)
    end;
delete_exchange_fun(Name, DeletionFun, PrePostCommitFun, khepri) ->
    fun() ->
            Fun = DeletionFun(khepri),
            execute_transaction(fun() ->
                                        case lookup_tx_in_khepri(Name) of
                                            [X] -> Fun(X, false);
                                            [] -> {error, not_found}
                                        end
                                end, PrePostCommitFun, khepri)
    end.                              

remove_bindings_in_mnesia(X = #exchange{name = XName}, OnlyDurable, RemoveBindingsForSource) ->
    Bindings = case RemoveBindingsForSource of
                   true  -> rabbit_binding:remove_for_source_in_mnesia(XName);
                   false -> []
               end,
    {deleted, X, Bindings, rabbit_binding:remove_for_destination_in_mnesia(XName, OnlyDurable)}.

remove_bindings_in_khepri(X = #exchange{name = XName}, OnlyDurable, RemoveBindingsForSource) ->
    Bindings = case RemoveBindingsForSource of
                   true  -> rabbit_binding:remove_for_source_in_khepri(XName);
                   false -> []
               end,
    {deleted, X, Bindings, rabbit_binding:remove_for_destination_in_khepri(XName, OnlyDurable)}.

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
                       true  -> store_exchange(X, mnesia);
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
              [_ = store_exchange(X, khepri) || X <- DurableExchanges],
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
