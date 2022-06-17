%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2012-2021 VMware, Inc. or its affiliates.  All rights reserved.
%% -----------------------------------------------------------------------------

%% JMS on Rabbit Selector Exchange plugin

%% -----------------------------------------------------------------------------
-module(rabbit_jms_topic_exchange).

-behaviour(rabbit_exchange_type).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include("rabbit_jms_topic_exchange.hrl").

%% Rabbit exchange type functions:
-export([ description/0
        , serialise_events/0
        , route/2
        , validate/1
        , create/2
        , delete/3
        , validate_binding/2
        , add_binding/3
        , remove_bindings/3
        , assert_args_equivalence/2
        , policy_changed/2 ]).

%% Initialisation of database function:
-export([setup_db_schema/0]).

-export([info/1, info/2]).

%%----------------------------------------------------------------------------

%% Register exchange type
-rabbit_boot_step({ ?MODULE
                  , [ {description, "exchange type JMS topic selector"}
                    , {mfa, {rabbit_registry, register, [exchange, ?X_TYPE_NAME, ?MODULE]}}
                    , {cleanup, {rabbit_registry, unregister, [exchange, ?X_TYPE_NAME]}}
                    , {requires, rabbit_registry}
                    , {enables, kernel_ready} ] }).

%% Initialise database
-rabbit_boot_step({ rabbit_jms_topic_exchange_mnesia
                  , [ {description, "database exchange type JMS topic selector"}
                    , {mfa, {?MODULE, setup_db_schema, []}}
                    , {requires, database}
                    , {enables, external_infrastructure} ] }).

-rabbit_feature_flag(
   {rabbit_jms_topic_exchange_raft_based_metadata_store,
    #{desc          => "Use the new Raft-based metadata store",
      doc_url       => "", %% TODO
      stability     => experimental,
      depends_on    => [raft_based_metadata_store_phase1],
      migration_fun => {?MODULE, mds_migration}
     }}).
%%----------------------------------------------------------------------------

% Initialise database table for all exchanges of type <<"x-jms-topic">>
setup_db_schema() ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              case mnesia:create_table( ?JMS_TOPIC_TABLE
                                      , [ {attributes, record_info(fields, ?JMS_TOPIC_RECORD)}
                                        , {record_name, ?JMS_TOPIC_RECORD}
                                        , {type, set} ]
                                      ) of
                  {atomic, ok} -> ok;
                  {aborted, {already_exists, ?JMS_TOPIC_TABLE}} -> ok
              end
      end,
      fun() ->
              ok
      end).

%%----------------------------------------------------------------------------
%% R E F E R E N C E   T Y P E   I N F O R M A T I O N

%% -type(binding() ::
%%         #binding{source      :: rabbit_exchange:name(),
%%                  destination :: binding_destination(),
%%                  key         :: rabbit_binding:key(),
%%                  args        :: rabbit_framing:amqp_table()}).
%%
%% -type(exchange() ::
%%         #exchange{name        :: rabbit_exchange:name(),
%%                   type        :: rabbit_exchange:type(),
%%                   durable     :: boolean(),
%%                   auto_delete :: boolean(),
%%                   arguments   :: rabbit_framing:amqp_table()}).
%%
%% -type(amqp_field_type() ::
%%       'longstr' | 'signedint' | 'decimal' | 'timestamp' |
%%       'table' | 'byte' | 'double' | 'float' | 'long' |
%%       'short' | 'bool' | 'binary' | 'void' | 'array').

%%----------------------------------------------------------------------------
%% E X P O R T E D   E X C H A N G E   B E H A V I O U R

% Exchange description
description() -> [ {name, <<"jms-selector">>}
                 , {description, <<"JMS selector exchange">>} ].

% Binding event serialisation
serialise_events() -> false.

% Route messages
route( #exchange{name = XName}
     , #delivery{message = #basic_message{content = MessageContent, routing_keys = RKs}}
     ) ->
  BindingFuns = get_binding_funs_x(XName),
  match_bindings(XName, RKs, MessageContent, BindingFuns).


% Before exchange declaration
validate(_X) -> ok.

% After exchange declaration and recovery
create(transaction, #exchange{name = XName}) ->
  add_initial_record(XName);
create(_Tx, _X) ->
  ok.

% Delete an exchange
delete(transaction, #exchange{name = XName}, _Bs) ->
  delete_state(XName),
  ok;
delete(_Tx, _X, _Bs) ->
  ok.

% Before add binding
validate_binding(_X, _B) -> ok.

% A new binding has ben added or recovered
add_binding( Tx
           , #exchange{name = XName}
           , #binding{key = BindingKey, destination = Dest, args = Args}
           ) ->
  Selector = get_string_arg(Args, ?RJMS_COMPILED_SELECTOR_ARG),
  BindGen = generate_binding_fun(Selector),
  case {Tx, BindGen} of
    {transaction, {ok, BindFun}} ->
      add_binding_fun(XName, {{BindingKey, Dest}, BindFun});
    {none, {error, _}} ->
      parsing_error(XName, Selector, Dest);
    _ ->
      ok
  end,
  ok.

% Binding removal
remove_bindings( transaction
               , #exchange{name = XName}
               , Bindings
               ) ->
  remove_binding_funs(XName, Bindings),
  ok;
remove_bindings(_Tx, _X, _Bs) ->
  ok.

% Exchange argument equivalence
assert_args_equivalence(X, Args) ->
  rabbit_exchange:assert_args_equivalence(X, Args).

% Policy change notifications ignored
policy_changed(_X1, _X2) -> ok.

% Stub for type-specific exchange information
info(_X) -> [].
info(_X, _) -> [].


mds_migration(FeatureName, FeatureProps, IsEnabled) ->
    TablesAndOwners = [{?JMS_TOPIC_TABLE, ?MODULE, #{}}],
    rabbit_core_ff:mds_migration(FeatureName, FeatureProps, TablesAndOwners, IsEnabled).

clear_data_in_khepri(?JMS_TOPIC_TABLE, _ExtraArgs) ->
    case rabbit_khepri:delete(khepri_jms_topic_exchange_path()) of
        {ok, _} ->
            ok;
        Error ->
            throw(Error)
    end.

mnesia_write_to_khepri(?JMS_TOPIC_TABLE, #?JMS_TOPIC_RECORD{x_name = XName, x_selector_funs = BFuns},
                       _ExtraArgs) ->
    case rabbit_khepri:create(khepri_jms_topic_exchange_path(XName), BFuns) of
        {ok, _} -> ok;
        {error, {mismatching_node, _}} -> ok;
        Error -> throw(Error)
    end.

mnesia_delete_to_khepri(?JMS_TOPIC_TABLE, #?JMS_TOPIC_RECORD{x_name = XName}, _ExtraArgs) ->
    case rabbit_khepri:delete(khepri_jms_topic_exchange_path(XName)) of
        {ok, _} ->
            ok;
        Error ->
            throw(Error)
    end;
mnesia_delete_to_khepri(?JMS_TOPIC_TABLE, Key, _ExtraArgs) ->
    case rabbit_khepri:delete(khepri_jms_topic_exchange_path(Key)) of
        {ok, _} ->
            ok;
        Error ->
            throw(Error)
    end.

%%----------------------------------------------------------------------------
%% P R I V A T E   F U N C T I O N S

% Get a string argument from the args or arguments parameters
get_string_arg(Args, ArgName) -> get_string_arg(Args, ArgName, error).

get_string_arg(Args, ArgName, Default) ->
  case rabbit_misc:table_lookup(Args, ArgName) of
    {longstr, BinVal} -> binary_to_list(BinVal);
    _ -> Default
  end.

% Match bindings for the current message
match_bindings( XName, _RoutingKeys, MessageContent, BindingFuns) ->
  MessageHeaders = get_headers(MessageContent),
  rabbit_router:match_bindings( XName
                              , fun(#binding{key = Key, destination = Dest}) ->
                                  binding_fun_match({Key, Dest}, MessageHeaders, BindingFuns)
                                end
                              ).

% Select binding function from Funs dictionary, apply it to Headers and return result (true|false)
binding_fun_match(DictKey, Headers, FunsDict) ->
  case dict:find(DictKey, FunsDict) of
    {ok, Fun} when is_function(Fun, 1) -> Fun(Headers);
    error                              -> false          % do not match if no function found
  end.

% get Headers from message content
get_headers(Content) ->
  case (Content#content.properties)#'P_basic'.headers of
    undefined -> [];
    H         -> rabbit_misc:sort_field_table(H)
  end.

% generate the function that checks the message against the selector
generate_binding_fun(ERL) ->
  case decode_term(ERL) of
    {error, _}    -> error;
    {ok, ErlTerm} -> check_fun(ErlTerm)
  end.

% build checking function from compiled expression
check_fun(CompiledExp) ->
  { ok,
    fun(Headers) ->
      selector_match(CompiledExp, Headers)
    end
  }.

% get an erlang term from a string
decode_term(Str) ->
  try
    {ok, Ts, _} = erl_scan:string(Str),
    {ok, Term} = erl_parse:parse_term(Ts),
    {ok, Term}
  catch
    Err -> {error, {invalid_erlang_term, Err}}
  end.

% Evaluate the selector and check against the Headers
selector_match(Selector, Headers) ->
  case sjx_evaluator:evaluate(Selector, Headers) of
    true -> true;
    _    -> false
  end.

% get binding funs from state (using dirty_reads)
get_binding_funs_x(XName) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              mnesia:async_dirty(
                fun() ->
                        #?JMS_TOPIC_RECORD{x_selector_funs = BindingFuns}
                            = read_state_in_mnesia(XName),
                        BindingFuns
                end,
                []
               )
      end,
      fun() ->
              read_state_in_khepri(XName)
      end).

add_initial_record(XName) ->
  write_state_fun(XName, dict:new()).

% add binding fun to binding fun dictionary
add_binding_fun(XName, BindingKeyAndFun) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              #?JMS_TOPIC_RECORD{x_selector_funs = BindingFuns} =
                  read_state_for_update_in_mnesia(XName),
              write_state_fun_in_mnesia(XName, put_item(BindingFuns, BindingKeyAndFun))
      end,
      fun() ->
              Path = khepri_jms_topic_exchange_path(XName),
              case rabbit_khepri:transaction(
                     fun() ->
                             case khepri_tx:get(Path) of
                                 {ok, #{Path := #{data := BindingFuns}}} ->
                                     {ok, _} = khepri_tx:put(Path, put_item(BindingFuns, BindingKeyAndFun)),
                                     ok;
                                 Err ->
                                     Err
                             end
                     end) of
                  ok -> ok;
                  _ -> exchange_state_corrupt_error(XName)
              end
      end).

% remove binding funs from binding fun dictionary
remove_binding_funs(XName, Bindings) ->
  BindingKeys = [ {BindingKey, DestName} || #binding{key = BindingKey, destination = DestName} <- Bindings ],
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              #?JMS_TOPIC_RECORD{x_selector_funs = BindingFuns} =
                  read_state_for_update_in_mnesia(XName),
              write_state_fun_in_mnesia(XName, remove_items(BindingFuns, BindingKeys))
      end,
      fun() ->
              Path = khepri_jms_topic_exchange_path(XName),
              case rabbit_khepri:transaction(
                     fun() ->
                             case khepri_tx:get(Path) of
                                 {ok, #{Path := #{data := BindingFuns}}} ->
                                     {ok, _} = khepri_tx:put(Path, remove_items(BindingFuns, BindingKeys)),
                                     ok;
                                 Err ->
                                     Err
                             end
                     end) of
                  ok -> ok;
                  _ -> exchange_state_corrupt_error(XName)
              end
      end).

% add an item to the dictionary of binding functions
put_item(Dict, {Key, Item}) -> dict:store(Key, Item, Dict).

% remove a list of keyed items from the dictionary, by key
remove_items(Dict, []) -> Dict;
remove_items(Dict, [Key | Keys]) -> remove_items(dict:erase(Key, Dict), Keys).

% delete all the state saved for this exchange
delete_state(XName) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              mnesia:delete(?JMS_TOPIC_TABLE, XName, write)
      end,
      fun() ->
              rabbit_khepri:delete(khepri_jms_topic_exchange_path(XName))
      end).

% Basic read for update
read_state_for_update_in_mnesia(XName) -> read_state_in_mnesia(XName, write).

% Basic read
read_state_in_mnesia(XName) -> read_state_in_mnesia(XName, read).

% Lockable read
read_state_in_mnesia(XName, Lock) ->
  case mnesia:read(?JMS_TOPIC_TABLE, XName, Lock) of
    [Rec] -> Rec;
    _     -> exchange_state_corrupt_error(XName)
  end.

read_state_in_khepri(XName) ->
    case rabbit_khepri:get(khepri_jms_topic_exchange_path(XName)) of
        {ok, #{data := BindingFuns}} ->
            BindingFuns;
        _ ->
            exchange_state_corrupt_error(XName)
    end.

% Basic write
write_state_fun(XName, BFuns) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              write_state_fun_in_mnesia(XName, BFuns)
      end,
      fun() ->
              {ok, _} = rabbit_khepri:put(khepri_jms_topic_exchange_path(XName), BFuns),
              ok
      end).

write_state_fun_in_mnesia(XName, BFuns) ->
    mnesia:write( ?JMS_TOPIC_TABLE
                , #?JMS_TOPIC_RECORD{x_name = XName, x_selector_funs = BFuns}
                , write ).

%%----------------------------------------------------------------------------
%% E R R O R S

% state error
exchange_state_corrupt_error(#resource{name = XName}) ->
  rabbit_misc:protocol_error( internal_error
                            , "exchange named '~s' has no saved state or incorrect saved state"
                            , [XName] ).

% parsing error
parsing_error(#resource{name = XName}, S, #resource{name = DestName}) ->
  rabbit_misc:protocol_error( precondition_failed
                            , "cannot parse selector '~p' binding destination '~s' to exchange '~s'"
                            , [S, DestName, XName] ).

%%----------------------------------------------------------------------------
khepri_jms_topic_exchange_path(#resource{virtual_host = VHost, name = Name}) ->
    [?MODULE, jms_topic_exchange, VHost, Name].

khepri_jms_topic_exchange_path() ->
    [?MODULE, jms_topic_exchange].
