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

execute_khepri_tx_with_tail(TxFun) ->
    case khepri_tx:is_transaction() of
        true  -> rabbit_khepri:transaction(TxFun);
        false -> TailFun = rabbit_khepri:transaction(TxFun),
                 TailFun()
    end.

%% TODO should rabbit table have a conversion of mnesia_table -> khepri_path?
table_filter_in_khepri(Pred, PrePostCommitFun, Path) ->
    lists:foldl(
      fun (E, Acc) ->
              case execute_khepri_transaction(
                     fun () -> khepri_tx:find(Path, #if_data_matches{pattern = E}) =/= []
                                   andalso Pred(E) end,
                     fun (false, _Tx) -> false;
                         (true,   Tx) -> PrePostCommitFun(E, Tx), true
                     end) of
                  false -> Acc;
                  true  -> [E | Acc]
              end
      end, [], khepri_read_all(Path)).

khepri_read_all(Path) ->
    case rabbit_khepri:list_child_data(Path) of
        {ok, Map} -> maps:values(Map);
        _         -> []
    end.

execute_khepri_transaction(TxFun, PrePostCommitFun) ->
    case khepri_tx:is_transaction() of
        true  -> throw(unexpected_transaction);
        false -> ok
    end,
    PrePostCommitFun(rabbit_khepri:transaction(
                       fun () ->
                               Result = TxFun(),
                               PrePostCommitFun(Result, true),
                               Result
                       end), false).

