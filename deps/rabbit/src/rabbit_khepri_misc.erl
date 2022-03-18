%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_khepri_misc).

-include_lib("khepri/include/khepri.hrl").

-export([execute_khepri_transaction/2]).
-export([execute_khepri_tx_with_tail/1]).
-export([table_filter_in_khepri/3]).
-export([retry/1]).

-define(WAIT_SECONDS, 30).

execute_khepri_tx_with_tail(TxFun) ->
    case khepri_tx:is_transaction() of
        true  -> rabbit_khepri:transaction(TxFun);
        false -> TailFun = rabbit_khepri:transaction(TxFun),
                 TailFun()
    end.

%% TODO should rabbit table have a conversion of mnesia_table -> khepri_path?
table_filter_in_khepri(Pred, PreCommitFun, Path) ->
    case khepri_tx:is_transaction() of
        true  -> throw(unexpected_transaction);
        false -> ok
    end,
    rabbit_khepri:transaction(
      fun () ->
              khepri_filter(Path, Pred, PreCommitFun)
      end).

khepri_filter(Path, Pred, PreCommitFun) ->
    case khepri_tx:list(Path) of
        {ok, Map} ->
            maps:fold(
              fun
                  (P, #{data := Data}, Acc) when P =:= Path ->
                      case Pred(Data) of
                          true ->
                              PreCommitFun(Data),
                              [Data | Acc];
                          false ->
                              Acc
                      end;
                  (_, _, Acc) ->
                      Acc
              end, [], Map);
        _ ->
            []
    end.

execute_khepri_transaction(TxFun, PostCommitFun) ->
    case khepri_tx:is_transaction() of
        true  -> throw(unexpected_transaction);
        false -> ok
    end,
    PostCommitFun(rabbit_khepri:transaction(
                    fun () ->
                            TxFun()
                    end)).

retry(Fun) ->
    Until = erlang:system_time(millisecond) + (?WAIT_SECONDS * 1000),
    retry(Fun, Until).

retry(Fun, Until) ->
    case Fun() of
        ok -> ok;
        {error, Reason} ->
            case erlang:system_time(millisecond) of
                V when V >= Until ->
                    throw({error, Reason});
                _ ->
                    retry(Fun, Until)
            end
    end.
