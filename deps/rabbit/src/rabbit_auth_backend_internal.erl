%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_auth_backend_internal).

-include_lib("kernel/include/logger.hrl").

-include_lib("khepri/include/khepri.hrl").

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/logging.hrl").

-include("internal_user.hrl").

-behaviour(rabbit_authn_backend).
-behaviour(rabbit_authz_backend).

-export([user_login_authentication/2, user_login_authorization/2,
         check_vhost_access/3, check_resource_access/4, check_topic_access/4,
         with_user/2, with_user_in_mnesia/2, with_user_in_khepri/2]).

-export([add_user/3, add_user/4, add_user/5, delete_user/2, lookup_user/1, exists/1,
         change_password/3, clear_password/2,
         hash_password/2, change_password_hash/2, change_password_hash/3,
         set_tags/3, set_permissions/6, clear_permissions/3,
         clear_vhost_permissions_in_khepri/2,
         clear_permissions_in_mnesia/2,
         clear_permissions_in_khepri/2,
         set_topic_permissions/6, clear_topic_permissions/3, clear_topic_permissions/4,
         clear_vhost_topic_permissions_in_khepri/2,
         clear_topic_permissions_in_mnesia/3,
         clear_topic_permissions_in_khepri/3, clear_topic_permissions_in_khepri_tx_fun/3,
         add_user_sans_validation/3, put_user/2, put_user/3,
         update_user/5,
         update_user_with_hash/5,
         add_user_sans_validation/6]).

-export([set_user_limits/3, clear_user_limits/3, is_over_connection_limit/1,
         is_over_channel_limit/1, get_user_limits/0, get_user_limits/1]).

-export([user_info_keys/0, perms_info_keys/0,
         user_perms_info_keys/0, vhost_perms_info_keys/0,
         user_vhost_perms_info_keys/0, all_users/0,
         user_topic_perms_info_keys/0, vhost_topic_perms_info_keys/0,
         user_vhost_topic_perms_info_keys/0,
         list_users/0, list_users/2, list_permissions/0,
         list_user_permissions/1, list_user_permissions/3,
         list_topic_permissions/0,
         list_vhost_permissions/1, list_vhost_permissions/3,
         list_vhost_permissions_in_khepri_tx_fun/1,
         list_user_vhost_permissions/2,
         list_user_topic_permissions/1, list_vhost_topic_permissions/1, list_user_vhost_topic_permissions/2,
         list_vhost_topic_permissions_in_khepri_tx_fun/1]).

-export([state_can_expire/0]).
-export([clear_data_in_khepri/0,
         mnesia_write_to_khepri/1,
         mnesia_delete_to_khepri/1]).
-export([khepri_users_path/0,
         khepri_user_path/1]).

%% for testing
-export([hashing_module_for_user/1, expand_topic_permission/2]).

-ifdef(TEST).
-export([lookup_user_in_mnesia/1,
         lookup_user_in_khepri/1,
         add_user_sans_validation_in_mnesia/2,
         add_user_sans_validation_in_khepri/2,
         update_user_in_mnesia/2,
         update_user_in_khepri/2,
         delete_user_in_mnesia/1,
         delete_user_in_khepri/1,
         all_users_in_mnesia/0,
         all_users_in_khepri/0,

         check_vhost_access_in_mnesia/2,
         check_vhost_access_in_khepri/2,
         check_resource_access_in_mnesia/4,
         check_resource_access_in_khepri/4,
         set_permissions_in_mnesia/3,
         set_permissions_in_khepri/3,
         list_permissions_in_mnesia/1,
         list_permissions_in_khepri/1,
         match_user_vhost/2,
         match_path_in_khepri/1,

         check_topic_access_in_mnesia/5,
         check_topic_access_in_khepri/5,
         set_topic_permissions_in_mnesia/4,
         set_topic_permissions_in_khepri/4,
         list_topic_permissions_in_mnesia/1,
         list_topic_permissions_in_khepri/1,
         match_user_vhost_topic_permission/3,
         clear_topic_permissions_in_mnesia/2,
         clear_topic_permissions_in_khepri/2,

         extract_user_permission_params/2,
         extract_topic_permission_params/2]).
-endif.

-import(rabbit_data_coercion, [to_atom/1, to_list/1, to_binary/1]).

%%----------------------------------------------------------------------------

-type regexp() :: binary().

%%----------------------------------------------------------------------------
%% Implementation of rabbit_auth_backend

%% Returns a password hashing module for the user record provided. If
%% there is no information in the record, we consider it to be legacy
%% (inserted by a version older than 3.6.0) and fall back to MD5, the
%% now obsolete hashing function.
hashing_module_for_user(User) ->
    ModOrUndefined = internal_user:get_hashing_algorithm(User),
    rabbit_password:hashing_mod(ModOrUndefined).

-define(BLANK_PASSWORD_REJECTION_MESSAGE,
        "user '~s' attempted to log in with a blank password, which is prohibited by the internal authN backend. "
        "To use TLS/x509 certificate-based authentication, see the rabbitmq_auth_mechanism_ssl plugin and configure the client to use the EXTERNAL authentication mechanism. "
        "Alternatively change the password for the user to be non-blank.").

with_user(Username, Thunk) ->
    fun() ->
            rabbit_khepri:try_mnesia_or_khepri(
              with_user_in_mnesia(Username, Thunk),
              with_user_in_khepri(Username, Thunk))
    end.

with_user_in_mnesia(Username, Thunk) ->
    fun () ->
            case mnesia:read({rabbit_user, Username}) of
                [_U] -> Thunk();
                []   -> mnesia:abort({no_such_user, Username})
            end
    end.

with_user_in_khepri(Username, Thunk) ->
    fun() ->
            Path = khepri_user_path(Username),
            case khepri_tx:exists(Path) of
                true  -> Thunk();
                false -> khepri_tx:abort({no_such_user, Username})
            end
    end.

%% For cases when we do not have a set of credentials,
%% namely when x509 (TLS) certificates are used. This should only be
%% possible when the EXTERNAL authentication mechanism is used, see
%% rabbit_auth_mechanism_plain:handle_response/2 and rabbit_reader:auth_phase/2.
user_login_authentication(Username, []) ->
    internal_check_user_login(Username, fun(_) -> true end);
%% For cases when we do have a set of credentials. rabbit_auth_mechanism_plain:handle_response/2
%% performs initial validation.
user_login_authentication(Username, AuthProps) ->
    case lists:keyfind(password, 1, AuthProps) of
        {password, <<"">>} ->
            {refused, ?BLANK_PASSWORD_REJECTION_MESSAGE,
             [Username]};
        {password, ""} ->
            {refused, ?BLANK_PASSWORD_REJECTION_MESSAGE,
             [Username]};
        {password, Cleartext} ->
            internal_check_user_login(
              Username,
              fun(User) ->
                  case internal_user:get_password_hash(User) of
                      <<Salt:4/binary, Hash/binary>> ->
                          Hash =:= rabbit_password:salted_hash(
                              hashing_module_for_user(User), Salt, Cleartext);
                      _ ->
                          false
                  end
              end);
        false -> exit({unknown_auth_props, Username, AuthProps})
    end.

state_can_expire() -> false.

user_login_authorization(Username, _AuthProps) ->
    case user_login_authentication(Username, []) of
        {ok, #auth_user{impl = Impl, tags = Tags}} -> {ok, Impl, Tags};
        Else                                       -> Else
    end.

internal_check_user_login(Username, Fun) ->
    Refused = {refused, "user '~s' - invalid credentials", [Username]},
    case lookup_user(Username) of
        {ok, User} ->
            Tags = internal_user:get_tags(User),
            case Fun(User) of
                true -> {ok, #auth_user{username = Username,
                                        tags     = Tags,
                                        impl     = none}};
                _    -> Refused
            end;
        {error, not_found} ->
            Refused
    end.

check_vhost_access(#auth_user{username = Username}, VHostPath, _AuthzData) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> check_vhost_access_in_mnesia(Username, VHostPath) end,
      fun() -> check_vhost_access_in_khepri(Username, VHostPath) end).

check_vhost_access_in_mnesia(Username, VHostPath) ->
    case mnesia:dirty_read({rabbit_user_permission,
                            #user_vhost{username     = Username,
                                        virtual_host = VHostPath}}) of
        []   -> false;
        [_R] -> true
    end.

check_vhost_access_in_khepri(Username, VHostPath) ->
    Path = khepri_user_permission_path(Username, VHostPath),
    rabbit_khepri:exists(Path).

check_resource_access(#auth_user{username = Username},
                      #resource{virtual_host = VHostPath, name = Name},
                      Permission,
                      _AuthContext) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              check_resource_access_in_mnesia(
                Username, VHostPath, Name, Permission)
      end,
      fun() ->
              check_resource_access_in_khepri(
                Username, VHostPath, Name, Permission)
      end).

check_resource_access_in_mnesia(Username, VHostPath, Name, Permission) ->
    case mnesia:dirty_read({rabbit_user_permission,
                            #user_vhost{username     = Username,
                                        virtual_host = VHostPath}}) of
        [] ->
            false;
        [#user_permission{permission = P}] ->
            do_check_resource_access(Name, Permission, P)
    end.

check_resource_access_in_khepri(Username, VHostPath, Name, Permission) ->
    Path = khepri_user_permission_path(Username, VHostPath),
    case rabbit_khepri:get_data(Path) of
        {ok, #user_permission{permission = P}} ->
            do_check_resource_access(Name, Permission, P);
        _ ->
            false
    end.

do_check_resource_access(Name, Permission, P) ->
    PermRegexp = case element(permission_index(Permission), P) of
                     %% <<"^$">> breaks Emacs' erlang mode
                     <<"">> -> <<$^, $$>>;
                     RE     -> RE
                 end,
    re:run(Name, PermRegexp, [{capture, none}]) =:= match.

check_topic_access(#auth_user{username = Username},
                   #resource{virtual_host = VHostPath, name = Name, kind = topic},
                   Permission,
                   Context) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() ->
              check_topic_access_in_mnesia(
                Username, VHostPath, Name, Permission, Context)
      end,
      fun() ->
              check_topic_access_in_khepri(
                Username, VHostPath, Name, Permission, Context)
      end).

check_topic_access_in_mnesia(
  Username, VHostPath, Name, Permission, Context) ->
    case mnesia:dirty_read({rabbit_topic_permission,
        #topic_permission_key{user_vhost = #user_vhost{username     = Username,
                                                       virtual_host = VHostPath},
                                                       exchange     = Name
                             }}) of
        [] ->
            true;
        [#topic_permission{permission = P}] ->
            do_check_topic_access(Permission, Context, P)
    end.

check_topic_access_in_khepri(
  Username, VHostPath, Name, Permission, Context) ->
    Path = khepri_topic_permission_path(Username, VHostPath, Name),
    case rabbit_khepri:get_data(Path) of
        {ok, #topic_permission{permission = P}} ->
            do_check_topic_access(Permission, Context, P);
        _ ->
            true
    end.

do_check_topic_access(Permission, Context, P) ->
    PermRegexp = case element(permission_index(Permission), P) of
                     %% <<"^$">> breaks Emacs' erlang mode
                     <<"">> -> <<$^, $$>>;
                     RE     -> RE
                 end,
    PermRegexpExpanded = expand_topic_permission(
                           PermRegexp,
                           maps:get(variable_map, Context, undefined)
                          ),
    re:run(maps:get(routing_key, Context), PermRegexpExpanded, [{capture, none}])
    =:= match.

expand_topic_permission(Permission, ToExpand) when is_map(ToExpand) ->
    Opening = <<"{">>,
    Closing = <<"}">>,
    ReplaceFun = fun(K, V, Acc) ->
                    Placeholder = <<Opening/binary, K/binary, Closing/binary>>,
                    binary:replace(Acc, Placeholder, V, [global])
                 end,
    maps:fold(ReplaceFun, Permission, ToExpand);
expand_topic_permission(Permission, _ToExpand) ->
    Permission.

permission_index(configure) -> #permission.configure;
permission_index(write)     -> #permission.write;
permission_index(read)      -> #permission.read.

%%----------------------------------------------------------------------------
%% Manipulation of the user database

validate_credentials(Username, Password) ->
    rabbit_credential_validation:validate(Username, Password).

validate_and_alternate_credentials(Username, Password, ActingUser, Fun) ->
    case validate_credentials(Username, Password) of
        ok           ->
            Fun(Username, Password, ActingUser);
        {error, Err} ->
            rabbit_log:error("Credential validation for '~s' failed!", [Username]),
            {error, Err}
    end.

-spec add_user(rabbit_types:username(), rabbit_types:password(),
               rabbit_types:username()) -> 'ok' | {'error', string()}.

add_user(Username, Password, ActingUser) ->
    validate_and_alternate_credentials(Username, Password, ActingUser,
                                       fun add_user_sans_validation/3).

-spec add_user(rabbit_types:username(), rabbit_types:password(),
               rabbit_types:username(), [atom()]) -> 'ok' | {'error', string()}.

add_user(Username, Password, ActingUser, Tags) ->
    add_user(Username, Password, ActingUser, undefined, Tags).

add_user(Username, Password, ActingUser, Limits, Tags) ->
    validate_and_alternate_credentials(Username, Password, ActingUser,
                                       add_user_sans_validation(Limits, Tags)).

add_user_sans_validation(Username, Password, ActingUser) ->
    add_user_sans_validation(Username, Password, ActingUser, undefined, []).

add_user_sans_validation(Limits, Tags) ->
    fun(Username, Password, ActingUser) ->
            add_user_sans_validation(Username, Password, ActingUser, Limits, Tags)
    end.

add_user_sans_validation(Username, Password, ActingUser, Limits, Tags) ->
    rabbit_log:debug("Asked to create a new user '~s', password length in bytes: ~p", [Username, bit_size(Password)]),
    %% hash_password will pick the hashing function configured for us
    %% but we also need to store a hint as part of the record, so we
    %% retrieve it here one more time
    HashingMod = rabbit_password:hashing_mod(),
    PasswordHash = hash_password(HashingMod, Password),
    User0 = internal_user:create_user(Username, PasswordHash, HashingMod),
    ConvertedTags = [rabbit_data_coercion:to_atom(I) || I <- Tags],
    User1 = internal_user:set_tags(User0, ConvertedTags),
    User = case Limits of
               undefined -> User1;
               Term -> internal_user:update_limits(add, User1, Term)
           end,
    add_user_sans_validation_in(Username, User, ConvertedTags, Limits, ActingUser).

add_user_sans_validation(Username, PasswordHash, HashingAlgorithm, Tags, Limits, ActingUser) ->
    rabbit_log:debug("Asked to create a new user '~s' with password hash", [Username]),
    ConvertedTags = [rabbit_data_coercion:to_atom(I) || I <- Tags],
    HashingMod = rabbit_password:hashing_mod(),
    User0 = internal_user:create_user(Username, PasswordHash, HashingMod),
    User1 = internal_user:set_tags(
              internal_user:set_password_hash(User0,
                                              PasswordHash, HashingAlgorithm),
              ConvertedTags),
    User = case Limits of
               undefined -> User1;
               Term -> internal_user:update_limits(add, User1, Term)
           end,
    add_user_sans_validation_in(Username, User, ConvertedTags, Limits, ActingUser).

add_user_sans_validation_in(Username, User, ConvertedTags, Limits, ActingUser) ->
    try
        R = rabbit_khepri:try_mnesia_or_khepri(
              fun() -> add_user_sans_validation_in_mnesia(Username, User) end,
              fun() -> add_user_sans_validation_in_khepri(Username, User) end),
        rabbit_log:info("Created user '~s'", [Username]),
        rabbit_event:notify(user_created, [{name, Username},
                                           {user_who_performed_action, ActingUser}]),
        case ConvertedTags of
            [] -> ok;
            _ -> notify_user_tags_set(Username, ConvertedTags, ActingUser)
        end,
        case Limits of
            undefined -> ok;
            _ -> notify_limit_set(Username, ActingUser, Limits)
        end,
        R
    catch
        throw:{error, {user_already_exists, _}} = Error ->
            rabbit_log:warning("Failed to add user '~s': the user already exists", [Username]),
            throw(Error);
        Class:Error:Stacktrace ->
            rabbit_log:warning("Failed to add user '~s': ~p", [Username, Error]),
            erlang:raise(Class, Error, Stacktrace)
    end .

add_user_sans_validation_in_mnesia(Username, User) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              case mnesia:wread({rabbit_user, Username}) of
                  [] ->
                      ok = mnesia:write(rabbit_user, User, write);
                  _ ->
                      mnesia:abort({user_already_exists, Username})
              end
      end),
    ok.

add_user_sans_validation_in_khepri(Username, User) ->
    Path = khepri_user_path(Username),
    case rabbit_khepri:create(Path, User) of
        {ok, _} ->
            ok;
        {error, {mismatching_node, _}} ->
            throw({error, {user_already_exists, Username}});
        {error, _} = Error ->
            throw(Error)
    end.

-spec delete_user(rabbit_types:username(), rabbit_types:username()) -> 'ok'.

delete_user(Username, ActingUser) ->
    rabbit_log:debug("Asked to delete user '~s'", [Username]),
    try
        R = rabbit_khepri:try_mnesia_or_khepri(
              fun() -> delete_user_in_mnesia(Username) end,
              fun() -> delete_user_in_khepri(Username) end),
        rabbit_log:info("Deleted user '~s'", [Username]),
        rabbit_event:notify(user_deleted,
                            [{name, Username},
                             {user_who_performed_action, ActingUser}]),
        R
    catch
        throw:{error, {no_such_user, _}} = Error ->
            rabbit_log:warning("Failed to delete user '~s': the user does not exist", [Username]),
            throw(Error);
        Class:Error:Stacktrace ->
            rabbit_log:warning("Failed to delete user '~s': ~p", [Username, Error]),
            erlang:raise(Class, Error, Stacktrace)
    end .

delete_user_in_mnesia(Username) ->
    rabbit_misc:execute_mnesia_transaction(
      with_user_in_mnesia(
        Username,
        fun () ->
                ok = mnesia:delete({rabbit_user, Username}),
                [ok = mnesia:delete_object(
                        rabbit_user_permission, R, write) ||
                 R <- mnesia:match_object(
                        rabbit_user_permission,
                        #user_permission{user_vhost = #user_vhost{
                                                         username = Username,
                                                         virtual_host = '_'},
                                         permission = '_'},
                        write)],
                UserTopicPermissionsQuery = match_user_vhost_topic_permission(Username, '_'),
                UserTopicPermissions = UserTopicPermissionsQuery(),
                [ok = mnesia:delete_object(rabbit_topic_permission, R, write) || R <- UserTopicPermissions],
                ok
        end)).

delete_user_in_khepri(Username) ->
    rabbit_khepri:transaction(
      with_user_in_khepri(
        Username,
        fun() ->
                Path = khepri_user_path(Username),
                case khepri_tx:delete(Path) of
                    {ok, _} -> ok;
                    Error   -> khepri_tx:abort(Error)
                end
        end)).

-spec lookup_user
        (rabbit_types:username()) ->
            rabbit_types:ok(internal_user:internal_user()) |
            rabbit_types:error('not_found').

lookup_user(Username) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> lookup_user_in_mnesia(Username) end,
      fun() -> lookup_user_in_khepri(Username) end).

lookup_user_in_mnesia(Username) ->
    rabbit_misc:dirty_read({rabbit_user, Username}).

lookup_user_in_khepri(Username) ->
    Path = khepri_user_path(Username),
    case rabbit_khepri:get_data(Path) of
        {ok, User} -> {ok, User};
        _          -> {error, not_found}
    end.

-spec exists(rabbit_types:username()) -> boolean().

exists(Username) ->
    case lookup_user(Username) of
        {error, not_found} -> false;
        _                  -> true
    end.

-spec change_password
        (rabbit_types:username(), rabbit_types:password(), rabbit_types:username()) -> 'ok'.

change_password(Username, Password, ActingUser) ->
    validate_and_alternate_credentials(Username, Password, ActingUser,
                                       fun change_password_sans_validation/3).

change_password_sans_validation(Username, Password, ActingUser) ->
    try
        rabbit_log:debug("Asked to change password of user '~s', new password length in bytes: ~p", [Username, bit_size(Password)]),
        HashingAlgorithm = rabbit_password:hashing_mod(),
        R = change_password_hash(Username,
                                 hash_password(rabbit_password:hashing_mod(),
                                               Password),
                                 HashingAlgorithm),
        rabbit_log:info("Successfully changed password for user '~s'", [Username]),
        rabbit_event:notify(user_password_changed,
                            [{name, Username},
                             {user_who_performed_action, ActingUser}]),
        R
    catch
        throw:{error, {no_such_user, _}} = Error ->
            rabbit_log:warning("Failed to change password for user '~s': the user does not exist", [Username]),
            throw(Error);
        Class:Error:Stacktrace ->
            rabbit_log:warning("Failed to change password for user '~s': ~p", [Username, Error]),
            erlang:raise(Class, Error, Stacktrace)
    end.

update_user(Username, Password, Tags, Limits, ActingUser) ->
    validate_and_alternate_credentials(Username, Password, ActingUser,
                                       update_user_sans_validation(Tags, Limits)).

update_user_sans_validation(Tags, Limits) ->
    fun(Username, Password, ActingUser) ->
            try
                rabbit_log:debug("Asked to change password of user '~s', new password length in bytes: ~p", [Username, bit_size(Password)]),
                HashingAlgorithm = rabbit_password:hashing_mod(),

                rabbit_log:debug("Asked to set user tags for user '~s' to ~p", [Username, Tags]),

                ConvertedTags = [rabbit_data_coercion:to_atom(I) || I <- Tags],
                R = update_user_with_hash(Username,
                                          hash_password(rabbit_password:hashing_mod(),
                                                        Password),
                                          HashingAlgorithm,
                                          ConvertedTags,
                                          Limits),
                rabbit_log:info("Successfully changed password for user '~s'", [Username]),
                rabbit_event:notify(user_password_changed,
                                    [{name, Username},
                                     {user_who_performed_action, ActingUser}]),

                notify_user_tags_set(Username, ConvertedTags, ActingUser),
                R
            catch
                throw:{error, {no_such_user, _}} = Error ->
                    rabbit_log:warning("Failed to change password for user '~s': the user does not exist", [Username]),
                    throw(Error);
                Class:Error:Stacktrace ->
                    rabbit_log:warning("Failed to change password for user '~s': ~p", [Username, Error]),
                    erlang:raise(Class, Error, Stacktrace)
            end
    end.

-spec clear_password(rabbit_types:username(), rabbit_types:username()) -> 'ok'.

clear_password(Username, ActingUser) ->
    rabbit_log:info("Clearing password for '~s'", [Username]),
    R = change_password_hash(Username, <<"">>),
    rabbit_event:notify(user_password_cleared,
                        [{name, Username},
                         {user_who_performed_action, ActingUser}]),
    R.

-spec hash_password
        (module(), rabbit_types:password()) -> rabbit_types:password_hash().

hash_password(HashingMod, Cleartext) ->
    rabbit_password:hash(HashingMod, Cleartext).

-spec change_password_hash
        (rabbit_types:username(), rabbit_types:password_hash()) -> 'ok'.

change_password_hash(Username, PasswordHash) ->
    change_password_hash(Username, PasswordHash, rabbit_password:hashing_mod()).


change_password_hash(Username, PasswordHash, HashingAlgorithm) ->
    update_user(Username,
                fun(User) ->
                        internal_user:set_password_hash(User, PasswordHash, HashingAlgorithm)
                end).

update_user_with_hash(Username, PasswordHash, HashingAlgorithm, ConvertedTags, Limits) ->
    update_user(Username,
                fun(User0) ->
                        User1 = internal_user:set_password_hash(User0,
                                                                PasswordHash, HashingAlgorithm),
                        User2 = case Limits of
                                    undefined -> User1;
                                    _         -> internal_user:update_limits(add, User1, Limits)
                                end,
                        internal_user:set_tags(User2, ConvertedTags)
                end).

-spec set_tags(rabbit_types:username(), [atom()], rabbit_types:username()) -> 'ok'.

set_tags(Username, Tags, ActingUser) ->
    ConvertedTags = [rabbit_data_coercion:to_atom(I) || I <- Tags],
    rabbit_log:debug("Asked to set user tags for user '~s' to ~p", [Username, ConvertedTags]),
    try
        R = update_user(Username, fun(User) ->
                                     internal_user:set_tags(User, ConvertedTags)
                                  end),
        notify_user_tags_set(Username, ConvertedTags, ActingUser),
        R
    catch
        throw:{error, {no_such_user, _}} = Error ->
            rabbit_log:warning("Failed to set tags for user '~s': the user does not exist", [Username]),
            throw(Error);
        Class:Error:Stacktrace ->
            rabbit_log:warning("Failed to set tags for user '~s': ~p", [Username, Error]),
            erlang:raise(Class, Error, Stacktrace)
    end .

notify_user_tags_set(Username, ConvertedTags, ActingUser) ->
    rabbit_log:info("Successfully set user tags for user '~s' to ~p", [Username, ConvertedTags]),
    rabbit_event:notify(user_tags_set, [{name, Username}, {tags, ConvertedTags},
                                        {user_who_performed_action, ActingUser}]).

-spec set_permissions
        (rabbit_types:username(), rabbit_types:vhost(), regexp(), regexp(),
         regexp(), rabbit_types:username()) ->
            'ok'.

set_permissions(Username, VirtualHost, ConfigurePerm, WritePerm, ReadPerm, ActingUser) ->
    rabbit_log:debug("Asked to set permissions for "
                     "'~s' in virtual host '~s' to '~s', '~s', '~s'",
                     [Username, VirtualHost, ConfigurePerm, WritePerm, ReadPerm]),
    lists:map(
      fun (RegexpBin) ->
              Regexp = binary_to_list(RegexpBin),
              case re:compile(Regexp) of
                  {ok, _}         -> ok;
                  {error, Reason} ->
                      rabbit_log:warning("Failed to set permissions for '~s' in virtual host '~s': "
                                         "regular expression '~s' is invalid",
                                         [Username, VirtualHost, RegexpBin]),
                      throw({error, {invalid_regexp, Regexp, Reason}})
              end
      end, [ConfigurePerm, WritePerm, ReadPerm]),
    UserPermission = #user_permission{
                        user_vhost = #user_vhost{
                                        username     = Username,
                                        virtual_host = VirtualHost},
                        permission = #permission{
                                        configure  = ConfigurePerm,
                                        write      = WritePerm,
                                        read       = ReadPerm}},
    try
        R = rabbit_khepri:try_mnesia_or_khepri(
              fun() ->
                      set_permissions_in_mnesia(
                        Username, VirtualHost, UserPermission)
              end,
              fun() ->
                      set_permissions_in_khepri(
                        Username, VirtualHost, UserPermission)
              end),
        rabbit_log:info("Successfully set permissions for "
                        "'~s' in virtual host '~s' to '~s', '~s', '~s'",
                        [Username, VirtualHost, ConfigurePerm, WritePerm, ReadPerm]),
        rabbit_event:notify(permission_created, [{user,      Username},
                                                 {vhost,     VirtualHost},
                                                 {configure, ConfigurePerm},
                                                 {write,     WritePerm},
                                                 {read,      ReadPerm},
                                                 {user_who_performed_action, ActingUser}]),
        R
    catch
        throw:{error, {no_such_vhost, _}} = Error ->
            rabbit_log:warning("Failed to set permissions for '~s': virtual host '~s' does not exist",
                               [Username, VirtualHost]),
            throw(Error);
        throw:{error, {no_such_user, _}} = Error ->
            rabbit_log:warning("Failed to set permissions for '~s': the user does not exist",
                               [Username]),
            throw(Error);
        Class:Error:Stacktrace ->
            rabbit_log:warning("Failed to set permissions for '~s' in virtual host '~s': ~p",
                               [Username, VirtualHost, Error]),
            erlang:raise(Class, Error, Stacktrace)
    end.

set_permissions_in_mnesia(Username, VirtualHost, UserPermission) ->
    rabbit_misc:execute_mnesia_transaction(
      rabbit_vhost:with_user_and_vhost_in_mnesia(
        Username, VirtualHost,
        fun () -> ok = mnesia:write(
                         rabbit_user_permission,
                         UserPermission,
                         write)
        end)).

set_permissions_in_khepri(Username, VirtualHost, UserPermission) ->
    rabbit_khepri:transaction(
      rabbit_vhost:with_user_and_vhost_in_khepri(
        Username, VirtualHost,
        fun() ->
                Path = khepri_user_permission_path(
                         #if_all{conditions =
                                 [Username,
                                  #if_node_exists{exists = true}]},
                         VirtualHost),
                %% TODO: Add a keep_while for the intermediate
                %% 'user_permissions' node so it is removed when its last
                %% children is removed.
                Extra = #{keep_while =>
                          #{rabbit_vhost:khepri_vhost_path(VirtualHost) =>
                            #if_node_exists{exists = true}}},
                Ret = khepri_tx:put(
                        Path, UserPermission, Extra),
                case Ret of
                    {ok, _} -> ok;
                    Error   -> khepri_tx:abort(Error)
                end
        end)).

-spec clear_permissions
        (rabbit_types:username(), rabbit_types:vhost(), rabbit_types:username()) -> 'ok'.

clear_permissions(Username, VirtualHost, ActingUser) ->
    rabbit_log:debug("Asked to clear permissions for '~s' in virtual host '~s'",
                     [Username, VirtualHost]),
    try
        R = rabbit_khepri:try_mnesia_or_khepri(
              fun() -> clear_permissions_in_mnesia(Username, VirtualHost) end,
              fun() -> clear_permissions_in_khepri(Username, VirtualHost) end),
        post_clear_permissions(Username, VirtualHost, ActingUser),
        R
    catch
        throw:{error, {no_such_vhost, _}} = Error ->
            rabbit_log:warning("Failed to clear permissions for '~s': virtual host '~s' does not exist",
                               [Username, VirtualHost]),
            throw(Error);
        throw:{error, {no_such_user, _}} = Error ->
            rabbit_log:warning("Failed to clear permissions for '~s': the user does not exist",
                               [Username]),
            throw(Error);
        Class:Error:Stacktrace ->
            rabbit_log:warning("Failed to clear permissions for '~s' in virtual host '~s': ~p",
                               [Username, VirtualHost, Error]),
            erlang:raise(Class, Error, Stacktrace)
    end.

clear_vhost_permissions_in_khepri(VirtualHost, ActingUser) ->
    rabbit_log:debug("Asked to clear permissions for everyone in virtual host '~s'",
                     [VirtualHost]),
    Path = khepri_user_permission_path(?STAR, VirtualHost),
    case rabbit_khepri:delete(Path) of
        {ok, Result} ->
            _ = maps:fold(
                  fun(Path1, _NodeProps, Acc) ->
                          Username = khepri_path_to_user(Path1),
                          post_clear_permissions(
                            Username, VirtualHost, ActingUser),
                          Acc
                  end, ok, Result),
            ok;
        Error ->
            Error
    end.

post_clear_permissions(Username, VirtualHost, ActingUser) ->
    rabbit_log:info("Successfully cleared permissions for '~s' in virtual host '~s'",
                    [Username, VirtualHost]),
    rabbit_event:notify(permission_deleted, [{user,  Username},
                                             {vhost, VirtualHost},
                                             {user_who_performed_action, ActingUser}]).

clear_permissions_in_mnesia(Username, VirtualHost) ->
    rabbit_misc:execute_mnesia_transaction(
      rabbit_vhost:with_user_and_vhost_in_mnesia(
        Username, VirtualHost,
        fun () ->
                ok = mnesia:delete({rabbit_user_permission,
                                    #user_vhost{username     = Username,
                                                virtual_host = VirtualHost}})
        end)).

clear_permissions_in_khepri(Username, VirtualHost) ->
    %% FIXME: We now check the vhost and user existence in an atomic manner.
    %%
    %% In the end, do we really need to have a more complex transaction
    %% mechanism when we just want to delete something? The code and the
    %% execution would be much simpler if we just deleted the permission and
    %% be done with it.
    rabbit_khepri:transaction(
      rabbit_vhost:with_user_and_vhost_in_khepri(
        Username, VirtualHost,
        fun () ->
                Path = khepri_user_permission_path(Username, VirtualHost),
                case khepri_tx:delete(Path) of
                    {ok, _} -> ok;
                    Error   -> khepri_tx:abort(Error)
                end
        end)).

update_user(Username, Fun) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> update_user_in_mnesia(Username, Fun) end,
      fun() -> update_user_in_khepri(Username, Fun) end).

update_user_in_mnesia(Username, Fun) ->
    rabbit_misc:execute_mnesia_transaction(
      with_user_in_mnesia(
        Username,
        fun () ->
                {ok, User} = lookup_user(Username),
                ok = mnesia:write(rabbit_user, Fun(User), write)
        end)).

update_user_in_khepri(Username, Fun) ->
    rabbit_khepri:transaction(
      with_user_in_khepri(
        Username,
        fun () ->
                Path = khepri_user_path(Username),
                {ok, #{Path := #{data := User}}} = khepri_tx:get(Path),
                case khepri_tx:put(Path, Fun(User)) of
                    {ok, #{Path := #{data := User}}} -> ok;
                    Error                            -> khepri_tx:abort(Error)
                end
        end)).

set_topic_permissions(Username, VirtualHost, Exchange, WritePerm, ReadPerm, ActingUser) ->
    rabbit_log:debug("Asked to set topic permissions on exchange '~s' for "
                     "user '~s' in virtual host '~s' to '~s', '~s'",
                     [Exchange, Username, VirtualHost, WritePerm, ReadPerm]),
    WritePermRegex = rabbit_data_coercion:to_binary(WritePerm),
    ReadPermRegex = rabbit_data_coercion:to_binary(ReadPerm),
    lists:map(
        fun (RegexpBin) ->
            case re:compile(RegexpBin) of
                {ok, _}         -> ok;
                {error, Reason} ->
                    rabbit_log:warning("Failed to set topic permissions on exchange '~s' for "
                                       "'~s' in virtual host '~s': regular expression '~s' is invalid",
                                       [Exchange, Username, VirtualHost, RegexpBin]),
                    throw({error, {invalid_regexp, RegexpBin, Reason}})
            end
        end, [WritePerm, ReadPerm]),
    TopicPermission = #topic_permission{
                         topic_permission_key = #topic_permission_key{
                                                   user_vhost = #user_vhost{
                                                                   username     = Username,
                                                                   virtual_host = VirtualHost},
                                                   exchange = Exchange
                                                  },
                         permission = #permission{
                                         write = WritePermRegex,
                                         read  = ReadPermRegex
                                        }
                        },
    try
        R = rabbit_khepri:try_mnesia_or_khepri(
              fun() ->
                      set_topic_permissions_in_mnesia(
                        Username, VirtualHost, Exchange, TopicPermission)
              end,
              fun() ->
                      set_topic_permissions_in_khepri(
                        Username, VirtualHost, Exchange, TopicPermission)
              end),
        rabbit_log:info("Successfully set topic permissions on exchange '~s' for "
                         "'~s' in virtual host '~s' to '~s', '~s'",
                         [Exchange, Username, VirtualHost, WritePerm, ReadPerm]),
        rabbit_event:notify(topic_permission_created, [
            {user,      Username},
            {vhost,     VirtualHost},
            {exchange,  Exchange},
            {write,     WritePermRegex},
            {read,      ReadPermRegex},
            {user_who_performed_action, ActingUser}]),
        R
    catch
        throw:{error, {no_such_vhost, _}} = Error ->
            rabbit_log:warning("Failed to set topic permissions on exchange '~s' for '~s': virtual host '~s' does not exist.",
                               [Exchange, Username, VirtualHost]),
            throw(Error);
        throw:{error, {no_such_user, _}} = Error ->
            rabbit_log:warning("Failed to set topic permissions on exchange '~s' for '~s': the user does not exist.",
                               [Exchange, Username]),
            throw(Error);
        Class:Error:Stacktrace ->
            rabbit_log:warning("Failed to set topic permissions on exchange '~s' for '~s' in virtual host '~s': ~p.",
                               [Exchange, Username, VirtualHost, Error]),
            erlang:raise(Class, Error, Stacktrace)
    end .

set_topic_permissions_in_mnesia(
  Username, VirtualHost, _Exchange, TopicPermission) ->
    rabbit_misc:execute_mnesia_transaction(
      rabbit_vhost:with_user_and_vhost_in_mnesia(
        Username, VirtualHost,
        fun () -> ok = mnesia:write(
                         rabbit_topic_permission,
                         TopicPermission,
                         write)
        end)).

set_topic_permissions_in_khepri(
  Username, VirtualHost, Exchange, TopicPermission) ->
    %% TODO: Add a keep_while for the intermediate 'topic_permissions' node so
    %% it is removed when its last children is removed.
    rabbit_khepri:transaction(
      rabbit_vhost:with_user_and_vhost_in_khepri(
        Username, VirtualHost,
        fun () ->
                Path = khepri_topic_permission_path(
                         #if_all{conditions =
                                 [Username,
                                  #if_node_exists{exists = true}]},
                         VirtualHost,
                         Exchange),
                Extra = #{keep_while =>
                          #{rabbit_vhost:khepri_vhost_path(VirtualHost) =>
                            #if_node_exists{exists = true}}},
                Ret = khepri_tx:put(Path, TopicPermission, Extra),
                case Ret of
                    {ok, _} -> ok;
                    Error   -> khepri_tx:abort(Error)
                end
        end)).

clear_topic_permissions(Username, VirtualHost, ActingUser) ->
    rabbit_log:debug("Asked to clear topic permissions for '~s' in virtual host '~s'",
                     [Username, VirtualHost]),
    try
        R = rabbit_khepri:try_mnesia_or_khepri(
              fun() ->
                      clear_topic_permissions_in_mnesia(
                        Username, VirtualHost)
              end,
              fun() ->
                      clear_topic_permissions_in_khepri(
                        Username, VirtualHost)
              end),
        post_clear_topic_permissions(Username, VirtualHost, ActingUser),
        R
    catch
        throw:{error, {no_such_vhost, _}} = Error ->
            rabbit_log:warning("Failed to clear topic permissions for '~s': virtual host '~s' does not exist",
                               [Username, VirtualHost]),
            throw(Error);
        throw:{error, {no_such_user, _}} = Error ->
            rabbit_log:warning("Failed to clear topic permissions for '~s': the user does not exist",
                               [Username]),
            throw(Error);
        Class:Error:Stacktrace ->
            rabbit_log:warning("Failed to clear topic permissions for '~s' in virtual host '~s': ~p",
                               [Username, VirtualHost, Error]),
            erlang:raise(Class, Error, Stacktrace)
    end.

clear_vhost_topic_permissions_in_khepri(VirtualHost, ActingUser) ->
    rabbit_log:debug("Asked to clear topic permissions for everyone in virtual host '~s'",
                     [VirtualHost]),
    Path = khepri_topic_permission_path(?STAR, VirtualHost, ?STAR),
    case rabbit_khepri:delete(Path) of
        {ok, Result} ->
            _ = maps:fold(
                  fun(Path1, _NodeProps, Acc) ->
                          Username = khepri_path_to_user(Path1),
                          post_clear_topic_permissions(
                            Username, VirtualHost, ActingUser),
                          Acc
                  end, ok, Result),
            ok;
        Error ->
            Error
    end.

post_clear_topic_permissions(Username, VirtualHost, ActingUser) ->
    rabbit_log:info("Successfully cleared topic permissions for '~s' in virtual host '~s'",
                    [Username, VirtualHost]),
    rabbit_event:notify(topic_permission_deleted, [{user,  Username},
                                                   {vhost, VirtualHost},
                                                   {user_who_performed_action, ActingUser}]).

clear_topic_permissions_in_mnesia(Username, VirtualHost) ->
    rabbit_misc:execute_mnesia_transaction(
      rabbit_vhost:with_user_and_vhost_in_mnesia(
        Username, VirtualHost,
        fun () ->
                ListFunction = match_user_vhost_topic_permission(Username, VirtualHost),
                List = ListFunction(),
                lists:foreach(fun(X) ->
                                      ok = mnesia:delete_object(rabbit_topic_permission, X, write)
                              end, List)
        end)).

clear_topic_permissions_in_khepri(Username, VirtualHost) ->
    clear_topic_permissions_in_khepri(Username, VirtualHost, ?STAR).

clear_topic_permissions(Username, VirtualHost, Exchange, ActingUser) ->
    rabbit_log:debug("Asked to clear topic permissions on exchange '~s' for '~s' in virtual host '~s'",
                     [Exchange, Username, VirtualHost]),
    try
        R = rabbit_khepri:try_mnesia_or_khepri(
              fun() ->
                      clear_topic_permissions_in_mnesia(
                        Username, VirtualHost, Exchange)
              end,
              fun() ->
                      clear_topic_permissions_in_khepri(
                        Username, VirtualHost, Exchange)
              end),
        rabbit_log:info("Successfully cleared topic permissions on exchange '~s' for '~s' in virtual host '~s'",
                        [Exchange, Username, VirtualHost]),
        rabbit_event:notify(permission_deleted, [{user,  Username},
                                                 {vhost, VirtualHost},
                                                 {user_who_performed_action, ActingUser}]),
        R
    catch
        throw:{error, {no_such_vhost, _}} = Error ->
            rabbit_log:warning("Failed to clear topic permissions on exchange '~s' for '~s': virtual host '~s' does not exist",
                               [Exchange, Username, VirtualHost]),
            throw(Error);
        throw:{error, {no_such_user, _}} = Error ->
            rabbit_log:warning("Failed to clear topic permissions on exchange '~s' for '~s': the user does not exist",
                               [Exchange, Username]),
            throw(Error);
        Class:Error:Stacktrace ->
            rabbit_log:warning("Failed to clear topic permissions on exchange '~s' for '~s' in virtual host '~s': ~p",
                               [Exchange, Username, VirtualHost, Error]),
            erlang:raise(Class, Error, Stacktrace)
    end.

clear_topic_permissions_in_mnesia(Username, VirtualHost, Exchange) ->
    rabbit_misc:execute_mnesia_transaction(
      rabbit_vhost:with_user_and_vhost_in_mnesia(
        Username, VirtualHost,
        fun () ->
                ok = mnesia:delete(rabbit_topic_permission,
                                   #topic_permission_key{
                                      user_vhost = #user_vhost{
                                                      username     = Username,
                                                      virtual_host = VirtualHost},
                                      exchange = Exchange
                                     }, write)
        end)).

clear_topic_permissions_in_khepri(Username, VirtualHost, Exchange) ->
    %% FIXME: We now check the vhost and user existence in an atomic manner.
    %%
    %% In the end, do we really need to have a more complex transaction
    %% mechanism when we just want to delete something? The code and the
    %% execution would be much simpler if we just deleted the permission and
    %% be done with it.
    rabbit_khepri:transaction(
      clear_topic_permissions_in_khepri_tx_fun(Username, VirtualHost, Exchange)).

clear_topic_permissions_in_khepri_tx_fun(Username, VirtualHost, Exchange) ->
    rabbit_vhost:with_user_and_vhost_in_khepri(
      Username, VirtualHost,
      fun () ->
              Path = khepri_topic_permission_path(
                       Username, VirtualHost, Exchange),
              case khepri_tx:delete(Path) of
                  {ok, _} -> ok;
                  Error   -> khepri_tx:abort(Error)
              end
      end).

put_user(User, ActingUser) -> put_user(User, undefined, ActingUser).

put_user(User, Version, ActingUser) ->
    Username        = maps:get(name, User),
    HasPassword     = maps:is_key(password, User),
    HasPasswordHash = maps:is_key(password_hash, User),
    Password        = maps:get(password, User, undefined),
    PasswordHash    = maps:get(password_hash, User, undefined),

    Tags            = case {maps:get(tags, User, undefined), maps:get(administrator, User, undefined)} of
                          {undefined, undefined} ->
                              throw({error, tags_not_present});
                          {undefined, AdminS} ->
                              case rabbit_misc:parse_bool(AdminS) of
                                  true  -> [administrator];
                                  false -> []
                              end;
                          {TagsVal, _} ->
                              tag_list_from(TagsVal)
                      end,

    %% pre-configured, only applies to newly created users
    Permissions     = maps:get(permissions, User, undefined),

    PassedCredentialValidation =
        case {HasPassword, HasPasswordHash} of
            {true, false} ->
                rabbit_credential_validation:validate(Username, Password) =:= ok;
            {false, true} -> true;
            _             ->
                rabbit_credential_validation:validate(Username, Password) =:= ok
        end,

    Limits = case rabbit_feature_flags:is_enabled(user_limits) of
                 false ->
                     undefined;
                 true ->
                     case maps:get(limits, User, undefined) of
                         undefined ->
                             undefined;
                         Term ->
                             case validate_user_limits(Term) of
                                 ok -> Term;
                                 Error -> throw(Error)
                             end
                     end
             end,
    case exists(Username) of
        true  ->
            case {HasPassword, HasPasswordHash} of
                {true, false} ->
                    update_user_password(PassedCredentialValidation, Username, Password, Tags, Limits, ActingUser);
                {false, true} ->
                    update_user_password_hash(Username, PasswordHash, Tags, Limits, User, Version);
                {true, true} ->
                    throw({error, both_password_and_password_hash_are_provided});
                %% clear password, update tags if needed
                _ ->
                    rabbit_auth_backend_internal:set_tags(Username, Tags, ActingUser),
                    rabbit_auth_backend_internal:clear_password(Username, ActingUser)
            end;
        false ->
            case {HasPassword, HasPasswordHash} of
                {true, false}  ->
                    create_user_with_password(PassedCredentialValidation, Username, Password, Tags, Permissions, Limits, ActingUser);
                {false, true}  ->
                    create_user_with_password_hash(Username, PasswordHash, Tags, User, Version, Permissions, Limits, ActingUser);
                {true, true}   ->
                    throw({error, both_password_and_password_hash_are_provided});
                {false, false} ->
                    %% this user won't be able to sign in using
                    %% a username/password pair but can be used for x509 certificate authentication,
                    %% with authn backends such as HTTP or LDAP and so on.
                    create_user_with_password(PassedCredentialValidation, Username, <<"">>, Tags, Permissions, Limits, ActingUser)
            end
    end.

update_user_password(_PassedCredentialValidation = true,  Username, Password, Tags, Limits, ActingUser) ->
    %% change_password, set_tags and limits
    rabbit_auth_backend_internal:update_user(Username, Password, Tags, Limits, ActingUser);
update_user_password(_PassedCredentialValidation = false, _Username, _Password, _Tags, _Limits, _ActingUser) ->
    %% we don't log here because
    %% rabbit_auth_backend_internal will do it
    throw({error, credential_validation_failed}).

update_user_password_hash(Username, PasswordHash, Tags, Limits, User, Version) ->
    %% when a hash this provided, credential validation
    %% is not applied
    HashingAlgorithm = hashing_algorithm(User, Version),

    Hash = rabbit_misc:b64decode_or_throw(PasswordHash),
    ConvertedTags = [rabbit_data_coercion:to_atom(I) || I <- Tags],
    rabbit_auth_backend_internal:update_user_with_hash(
      Username, Hash, HashingAlgorithm, ConvertedTags, Limits).

create_user_with_password(_PassedCredentialValidation = true,  Username, Password, Tags, undefined, Limits, ActingUser) ->
    rabbit_auth_backend_internal:add_user(Username, Password, ActingUser, Limits, Tags);
create_user_with_password(_PassedCredentialValidation = true,  Username, Password, Tags, PreconfiguredPermissions, Limits, ActingUser) ->
    rabbit_auth_backend_internal:add_user(Username, Password, ActingUser, Limits, Tags),
    preconfigure_permissions(Username, PreconfiguredPermissions, ActingUser);
create_user_with_password(_PassedCredentialValidation = false, _Username, _Password, _Tags, _, _, _) ->
    %% we don't log here because
    %% rabbit_auth_backend_internal will do it
    throw({error, credential_validation_failed}).

create_user_with_password_hash(Username, PasswordHash, Tags, User, Version, PreconfiguredPermissions, Limits, ActingUser) ->
    %% when a hash this provided, credential validation
    %% is not applied
    HashingAlgorithm = hashing_algorithm(User, Version),
    Hash             = rabbit_misc:b64decode_or_throw(PasswordHash),

    rabbit_auth_backend_internal:add_user_sans_validation(Username, Hash, HashingAlgorithm, Tags, Limits, ActingUser),
    preconfigure_permissions(Username, PreconfiguredPermissions, ActingUser).

preconfigure_permissions(_Username, undefined, _ActingUser) ->
    ok;
preconfigure_permissions(Username, Map, ActingUser) when is_map(Map) ->
    maps:map(fun(VHost, M) ->
                     rabbit_auth_backend_internal:set_permissions(Username, VHost,
                                                  maps:get(<<"configure">>, M),
                                                  maps:get(<<"write">>,     M),
                                                  maps:get(<<"read">>,      M),
                                                  ActingUser)
             end,
             Map),
    ok.

set_user_limits(Username, Definition, ActingUser) when is_list(Definition); is_binary(Definition) ->
    case rabbit_feature_flags:is_enabled(user_limits) of
        true  ->
            case rabbit_json:try_decode(rabbit_data_coercion:to_binary(Definition)) of
                {ok, Term} ->
                    validate_parameters_and_update_limit(Username, Term, ActingUser);
                {error, Reason} ->
                    {error_string, rabbit_misc:format(
                                     "JSON decoding error. Reason: ~ts", [Reason])}
            end;
        false -> {error_string, "cannot set any user limits: the user_limits feature flag is not enabled"}
    end;
set_user_limits(Username, Definition, ActingUser) when is_map(Definition) ->
    case rabbit_feature_flags:is_enabled(user_limits) of
        true  -> validate_parameters_and_update_limit(Username, Definition, ActingUser);
        false -> {error_string, "cannot set any user limits: the user_limits feature flag is not enabled"}
    end.

validate_parameters_and_update_limit(Username, Term, ActingUser) ->
    case validate_user_limits(Term) of
        ok ->
            update_user(Username, fun(User) ->
                                      internal_user:update_limits(add, User, Term)
                                  end),
            notify_limit_set(Username, ActingUser, Term);
        {errors, [{Reason, Arguments}]} ->
            {error_string, rabbit_misc:format(Reason, Arguments)}
    end.

validate_user_limits(Term) ->
    flatten_errors(rabbit_parameter_validation:proplist(
                     <<"user-limits">>, user_limit_validation(), Term)).

user_limit_validation() ->
    [{<<"max-connections">>, fun rabbit_parameter_validation:integer/2, optional},
     {<<"max-channels">>, fun rabbit_parameter_validation:integer/2, optional}].

clear_user_limits(Username, <<"all">>, ActingUser) ->
    update_user(Username, fun(User) ->
                              internal_user:clear_limits(User)
                          end),
    notify_limit_clear(Username, ActingUser);
clear_user_limits(Username, LimitType, ActingUser) ->
    update_user(Username, fun(User) ->
                              internal_user:update_limits(remove, User, LimitType)
                          end),
    notify_limit_clear(Username, ActingUser).

tag_list_from(Tags) when is_list(Tags) ->
    [to_atom(string:strip(to_list(T))) || T <- Tags];
tag_list_from(Tags) when is_binary(Tags) ->
    [to_atom(string:strip(T)) || T <- string:tokens(to_list(Tags), ",")].

flatten_errors(L) ->
    case [{F, A} || I <- lists:flatten([L]), {error, F, A} <- [I]] of
        [] -> ok;
        E  -> {errors, E}
    end.

%%----------------------------------------------------------------------------
%% Listing

-define(PERMS_INFO_KEYS, [configure, write, read]).
-define(USER_INFO_KEYS, [user, tags]).

-spec user_info_keys() -> rabbit_types:info_keys().

user_info_keys() -> ?USER_INFO_KEYS.

-spec perms_info_keys() -> rabbit_types:info_keys().

perms_info_keys()            -> [user, vhost | ?PERMS_INFO_KEYS].

-spec vhost_perms_info_keys() -> rabbit_types:info_keys().

vhost_perms_info_keys()      -> [user | ?PERMS_INFO_KEYS].

-spec user_perms_info_keys() -> rabbit_types:info_keys().

user_perms_info_keys()       -> [vhost | ?PERMS_INFO_KEYS].

-spec user_vhost_perms_info_keys() -> rabbit_types:info_keys().

user_vhost_perms_info_keys() -> ?PERMS_INFO_KEYS.

topic_perms_info_keys()            -> [user, vhost, exchange, write, read].
user_topic_perms_info_keys()       -> [vhost, exchange, write, read].
vhost_topic_perms_info_keys()      -> [user, exchange, write, read].
user_vhost_topic_perms_info_keys() -> [exchange, write, read].

all_users() ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> all_users_in_mnesia() end,
      fun() -> all_users_in_khepri() end).

all_users_in_mnesia() ->
    mnesia:dirty_match_object(rabbit_user, internal_user:pattern_match_all()).

all_users_in_khepri() ->
    Path = khepri_users_path(),
    case rabbit_khepri:list_child_data(Path) of
        {ok, Users} -> maps:values(Users);
        _           -> []
    end.

-spec list_users() -> [rabbit_types:infos()].

list_users() ->
    [extract_internal_user_params(U) ||
        U <- all_users()].

-spec list_users(reference(), pid()) -> 'ok'.

list_users(Ref, AggregatorPid) ->
    rabbit_control_misc:emitting_map(
      AggregatorPid, Ref,
      fun(U) -> extract_internal_user_params(U) end,
      all_users()).

-spec list_permissions() -> [rabbit_types:infos()].

list_permissions() ->
    MnesiaThunk = match_user_vhost('_', '_'),
    KhepriThunk = match_path_in_khepri(
                    khepri_user_permission_path(?STAR, ?STAR)),
    list_permissions(perms_info_keys(), MnesiaThunk, KhepriThunk).

list_permissions(Keys, MnesiaThunk, KhepriThunk) ->
    UserPermissions = rabbit_khepri:try_mnesia_or_khepri(
                        fun() -> list_permissions_in_mnesia(MnesiaThunk) end,
                        fun() -> list_permissions_in_khepri(KhepriThunk) end),
    [extract_user_permission_params(Keys, U) || U <- UserPermissions].

list_permissions_in_mnesia(QueryThunk) when is_function(QueryThunk) ->
    rabbit_misc:execute_mnesia_transaction(QueryThunk).

list_permissions_in_khepri(QueryThunk) when is_function(QueryThunk) ->
    case rabbit_khepri:transaction(QueryThunk, ro) of
        {ok, UserPermissions} -> maps:values(UserPermissions);
        _                     -> []
    end.

list_permissions(Keys, MnesiaThunk, KhepriThunk, Ref, AggregatorPid) ->
    Users = rabbit_khepri:try_mnesia_or_khepri(
              fun() -> list_permissions_in_mnesia(MnesiaThunk) end,
              fun() -> list_permissions_in_khepri(KhepriThunk) end),
    rabbit_control_misc:emitting_map(
      AggregatorPid, Ref, fun(U) -> extract_user_permission_params(Keys, U) end,
      Users).

filter_props(Keys, Props) -> [T || T = {K, _} <- Props, lists:member(K, Keys)].

-spec list_user_permissions
        (rabbit_types:username()) -> [rabbit_types:infos()].

list_user_permissions(Username) ->
    MnesiaThunk = with_user_in_mnesia(
                    Username, match_user_vhost(Username, '_')),
    KhepriThunk = with_user_in_khepri(
                    Username,
                    match_path_in_khepri(
                      khepri_user_permission_path(Username, ?STAR))),
    list_permissions(user_perms_info_keys(), MnesiaThunk, KhepriThunk).

-spec list_user_permissions
        (rabbit_types:username(), reference(), pid()) -> 'ok'.

list_user_permissions(Username, Ref, AggregatorPid) ->
    MnesiaThunk = with_user_in_mnesia(
                    Username, match_user_vhost(Username, '_')),
    KhepriThunk = with_user_in_khepri(
                    Username,
                    match_path_in_khepri(
                      khepri_user_permission_path(Username, ?STAR))),
    list_permissions(
      user_perms_info_keys(), MnesiaThunk, KhepriThunk, Ref, AggregatorPid).

-spec list_vhost_permissions
        (rabbit_types:vhost()) -> [rabbit_types:infos()].

list_vhost_permissions(VHostPath) ->
    MnesiaThunk = rabbit_vhost:with_in_mnesia(
                   VHostPath, match_user_vhost('_', VHostPath)),
    KhepriThunk = rabbit_vhost:with_in_khepri(
                    VHostPath,
                    match_path_in_khepri(
                      khepri_user_permission_path(?STAR, VHostPath))),
    list_permissions(vhost_perms_info_keys(), MnesiaThunk, KhepriThunk).

list_vhost_permissions_in_khepri_tx_fun(VHostPath) ->
    Fun = rabbit_vhost:with_in_khepri(
            VHostPath,
            match_path_in_khepri(
              khepri_user_permission_path(?STAR, VHostPath))),
    case Fun() of
        {ok, UserPermissions} ->
            [extract_user_permission_params(vhost_perms_info_keys(), U)
             || U <- maps:values(UserPermissions)];
        _ ->
            []
    end.

-spec list_vhost_permissions
        (rabbit_types:vhost(), reference(), pid()) -> 'ok'.

list_vhost_permissions(VHostPath, Ref, AggregatorPid) ->
    MnesiaThunk = rabbit_vhost:with_in_mnesia(
                    VHostPath, match_user_vhost('_', VHostPath)),
    KhepriThunk = rabbit_vhost:with_in_khepri(
                    VHostPath,
                    match_path_in_khepri(
                      khepri_user_permission_path(?STAR, VHostPath))),
    list_permissions(
      vhost_perms_info_keys(), MnesiaThunk, KhepriThunk, Ref, AggregatorPid).

-spec list_user_vhost_permissions
        (rabbit_types:username(), rabbit_types:vhost()) -> [rabbit_types:infos()].

list_user_vhost_permissions(Username, VHostPath) ->
    MnesiaThunk = rabbit_vhost:with_user_and_vhost_in_mnesia(
                    Username, VHostPath,
                    match_user_vhost(Username, VHostPath)),
    KhepriThunk = rabbit_vhost:with_user_and_vhost_in_khepri(
                    Username, VHostPath,
                    match_path_in_khepri(
                      khepri_user_permission_path(Username, VHostPath))),
    list_permissions(user_vhost_perms_info_keys(), MnesiaThunk, KhepriThunk).

extract_user_permission_params(Keys, #user_permission{
                                        user_vhost =
                                            #user_vhost{username     = Username,
                                                        virtual_host = VHostPath},
                                        permission = #permission{
                                                        configure = ConfigurePerm,
                                                        write     = WritePerm,
                                                        read      = ReadPerm}}) ->
    filter_props(Keys, [{user,      Username},
                        {vhost,     VHostPath},
                        {configure, ConfigurePerm},
                        {write,     WritePerm},
                        {read,      ReadPerm}]).

extract_internal_user_params(User) ->
    [{user, internal_user:get_username(User)},
     {tags, internal_user:get_tags(User)}].

match_user_vhost(Username, VHostPath) ->
    fun () -> mnesia:match_object(
                rabbit_user_permission,
                #user_permission{user_vhost = #user_vhost{
                                   username     = Username,
                                   virtual_host = VHostPath},
                                 permission = '_'},
                read)
    end.

match_path_in_khepri(Path) ->
    fun() -> rabbit_khepri:tx_match_and_get_data(Path) end.

list_topic_permissions() ->
    QueryThunk = match_user_vhost_topic_permission('_', '_'),
    Path = match_path_in_khepri(khepri_topic_permission_path(?STAR, ?STAR, ?STAR)),
    list_topic_permissions(topic_perms_info_keys(), QueryThunk, Path).

list_user_topic_permissions(Username) ->
    MnesiaThunk = with_user_in_mnesia(
                    Username,
                    match_user_vhost_topic_permission(Username, '_')),
    KhepriThunk = with_user_in_khepri(
                    Username,
                    match_path_in_khepri(
                      khepri_topic_permission_path(Username, ?STAR, ?STAR))),
    list_topic_permissions(
      user_topic_perms_info_keys(), MnesiaThunk, KhepriThunk).

list_vhost_topic_permissions(VHost) ->
    MnesiaThunk = rabbit_vhost:with_in_mnesia(
                    VHost, match_user_vhost_topic_permission('_', VHost)),
    KhepriThunk = rabbit_vhost:with_in_khepri(
                    VHost,
                    match_path_in_khepri(
                      khepri_topic_permission_path(?STAR, VHost, ?STAR))),
    list_topic_permissions(
      vhost_topic_perms_info_keys(), MnesiaThunk, KhepriThunk).

list_vhost_topic_permissions_in_khepri_tx_fun(VHost) ->
    Fun = rabbit_vhost:with_in_khepri(
            VHost,
            match_path_in_khepri(
              khepri_topic_permission_path(?STAR, VHost, ?STAR))),
    case Fun() of
        {ok, TopicPermissions} ->
            [extract_topic_permission_params(vhost_topic_perms_info_keys(), U)
             || U <- maps:values(TopicPermissions)];
        _ ->
            []
    end.

list_user_vhost_topic_permissions(Username, VHost) ->
    MnesiaThunk = rabbit_vhost:with_user_and_vhost_in_mnesia(
                    Username, VHost,
                    match_user_vhost_topic_permission(Username, VHost)),
    KhepriThunk = rabbit_vhost:with_user_and_vhost_in_khepri(
                    Username, VHost,
                    match_path_in_khepri(
                      khepri_topic_permission_path(Username, VHost, ?STAR))),
    list_topic_permissions(
      user_vhost_topic_perms_info_keys(), MnesiaThunk, KhepriThunk).

list_topic_permissions(Keys, MnesiaThunk, KhepriThunk) ->
    TopicPermissions = rabbit_khepri:try_mnesia_or_khepri(
                         fun() ->
                                 list_topic_permissions_in_mnesia(MnesiaThunk)
                         end,
                         fun() ->
                                 list_topic_permissions_in_khepri(KhepriThunk)
                         end),
    [extract_topic_permission_params(Keys, U) || U <- TopicPermissions].

list_topic_permissions_in_mnesia(QueryThunk) when is_function(QueryThunk) ->
    rabbit_misc:execute_mnesia_transaction(QueryThunk).

list_topic_permissions_in_khepri(QueryThunk) when is_function(QueryThunk) ->
    case rabbit_khepri:transaction(QueryThunk, ro) of
        {ok, TopicPermissions} -> maps:values(TopicPermissions);
        _                      -> []
    end.

match_user_vhost_topic_permission(Username, VHostPath) ->
    match_user_vhost_topic_permission(Username, VHostPath, '_').

match_user_vhost_topic_permission(Username, VHostPath, Exchange) ->
    fun () -> mnesia:match_object(
        rabbit_topic_permission,
        #topic_permission{topic_permission_key = #topic_permission_key{
            user_vhost = #user_vhost{
                username     = Username,
                virtual_host = VHostPath},
            exchange = Exchange},
            permission = '_'},
        read)
    end.

extract_topic_permission_params(Keys, #topic_permission{
            topic_permission_key = #topic_permission_key{
                                    user_vhost = #user_vhost{username     = Username,
                                                             virtual_host = VHostPath},
                                    exchange = Exchange},
            permission = #permission{
                write     = WritePerm,
                read      = ReadPerm}}) ->
    filter_props(Keys, [{user,      Username},
        {vhost,     VHostPath},
        {exchange,  Exchange},
        {write,     WritePerm},
        {read,      ReadPerm}]).

hashing_algorithm(User, Version) ->
    case maps:get(hashing_algorithm, User, undefined) of
        undefined ->
            case Version of
                %% 3.6.1 and later versions are supposed to have
                %% the algorithm exported and thus not need a default
                <<"3.6.0">>          -> rabbit_password_hashing_sha256;
                <<"3.5.", _/binary>> -> rabbit_password_hashing_md5;
                <<"3.4.", _/binary>> -> rabbit_password_hashing_md5;
                <<"3.3.", _/binary>> -> rabbit_password_hashing_md5;
                <<"3.2.", _/binary>> -> rabbit_password_hashing_md5;
                <<"3.1.", _/binary>> -> rabbit_password_hashing_md5;
                <<"3.0.", _/binary>> -> rabbit_password_hashing_md5;
                _                    -> rabbit_password:hashing_mod()
            end;
        Alg       -> rabbit_data_coercion:to_atom(Alg, utf8)
    end.

is_over_connection_limit(Username) ->
    Fun = fun() ->
              rabbit_connection_tracking:count_tracked_items_in({user, Username})
          end,
    is_over_limit(Username, <<"max-connections">>, Fun).

is_over_channel_limit(Username) ->
    Fun = fun() ->
              rabbit_channel_tracking:count_tracked_items_in({user, Username})
          end,
    is_over_limit(Username, <<"max-channels">>, Fun).

is_over_limit(Username, LimitType, Fun) ->
    case get_user_limit(Username, LimitType) of
        undefined -> false;
        {ok, 0} -> {true, 0};
        {ok, Limit} ->
            case Fun() >= Limit of
                false -> false;
                true -> {true, Limit}
            end
    end.

get_user_limit(Username, LimitType) ->
    case lookup_user(Username) of
        {ok, User} ->
            case rabbit_misc:pget(LimitType, internal_user:get_limits(User)) of
                undefined -> undefined;
                N when N < 0  -> undefined;
                N when N >= 0 -> {ok, N}
            end;
        _ ->
            undefined
    end.

get_user_limits() ->
    [{internal_user:get_username(U), internal_user:get_limits(U)} ||
        U <- all_users(),
        internal_user:get_limits(U) =/= #{}].

get_user_limits(Username) ->
    case lookup_user(Username) of
        {ok, User} -> internal_user:get_limits(User);
        _ -> undefined
    end.

notify_limit_set(Username, ActingUser, Term) ->
    rabbit_event:notify(user_limits_set,
        [{name, <<"limits">>},  {user_who_performed_action, ActingUser},
        {username, Username}  | maps:to_list(Term)]).

notify_limit_clear(Username, ActingUser) ->
    rabbit_event:notify(user_limits_cleared,
        [{name, <<"limits">>}, {user_who_performed_action, ActingUser},
        {username, Username}]).

clear_data_in_khepri() ->
    Path = khepri_users_path(),
    case rabbit_khepri:delete(Path) of
        {ok, _} -> ok;
        Error -> throw(Error)
    end.

mnesia_write_to_khepri(User) when ?is_internal_user(User) ->
    Username = internal_user:get_username(User),
    Path = khepri_user_path(Username),
    case rabbit_khepri:put(Path, User) of
        {ok, _} -> ok;
        Error -> throw(Error)
    end;
mnesia_write_to_khepri(
  #user_permission{
     user_vhost = #user_vhost{
                     username = Username,
                     virtual_host = VHost}} = UserPermission) ->
    Path = khepri_user_permission_path(
             #if_all{conditions =
                     [Username,
                      #if_node_exists{exists = true}]},
             VHost),
    Extra = #{keep_while =>
              #{rabbit_vhost:khepri_vhost_path(VHost) =>
                #if_node_exists{exists = true}}},
    case rabbit_khepri:put(Path, UserPermission, Extra) of
        {ok, _} -> ok;
        Error   -> throw(Error)
    end;
mnesia_write_to_khepri(
  #topic_permission{
     topic_permission_key =
     #topic_permission_key{
        user_vhost = #user_vhost{
                        username = Username,
                        virtual_host = VHost},
        exchange = Exchange}} = TopicPermission) ->
    Path = khepri_topic_permission_path(
             #if_all{conditions =
                     [Username,
                      #if_node_exists{exists = true}]},
             VHost,
             Exchange),
    Extra = #{keep_while =>
              #{rabbit_vhost:khepri_vhost_path(VHost) =>
                #if_node_exists{exists = true}}},
    case rabbit_khepri:put(Path, TopicPermission, Extra) of
        {ok, _} -> ok;
        Error   -> throw(Error)
    end.

mnesia_delete_to_khepri(User) when ?is_internal_user(User) ->
    Username = internal_user:get_username(User),
    Path = khepri_user_path(Username),
    case rabbit_khepri:delete(Path) of
        {ok, _} -> ok;
        Error -> throw(Error)
    end;
mnesia_delete_to_khepri(
  #user_permission{
     user_vhost = #user_vhost{
                     username = Username,
                     virtual_host = VHost}}) ->
    Path = khepri_user_permission_path(Username, VHost),
    case rabbit_khepri:delete(Path) of
        {ok, _} -> ok;
        Error -> throw(Error)
    end;
mnesia_delete_to_khepri(
  #topic_permission{
     topic_permission_key =
     #topic_permission_key{
        user_vhost = #user_vhost{
                        username = Username,
                        virtual_host = VHost},
        exchange = Exchange}}) ->
    Path = khepri_topic_permission_path(Username, VHost, Exchange),
    case rabbit_khepri:delete(Path) of
        {ok, _} -> ok;
        Error -> throw(Error)
    end.

khepri_users_path()        -> [?MODULE, users].
khepri_user_path(Username) -> [?MODULE, users, Username].

khepri_user_permission_path(Username, VHostName) ->
    [?MODULE, users, Username, user_permissions, VHostName].

khepri_topic_permission_path(Username, VHostName, Exchange) ->
    [?MODULE, users, Username, topic_permissions, VHostName, Exchange].

khepri_path_to_user([?MODULE, users, Username | _]) -> Username.
