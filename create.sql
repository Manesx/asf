create table if not exists wait_actions
(
    user_id integer not null,
    action  text not null
);

create table if not exists users
(
    id serial,

    user_id     integer not null,
    first_name  text    not null,
    age       integer,
    city        text,
    about       text,
    media      text,
    coordinates text,
    sex         integer not null,
    search_sex  integer not null,
    is_hide   boolean not null
);

create table if not exists ads
(
    id serial,

    group_id integer not null,
    token text not null,
    is_hide boolean not null,

    text text,
    media text,
    link text,

    duration integer not null
);

create table if not exists messages
(
    user_id    integer not null,
    message_id integer not null,
    reaction_id integer not null
);

create table if not exists reactions
(
    id serial,

    initiator_id integer not null,
    rated_id     integer not null,

    is_ads boolean not null,
    initiator_is_author boolean not null,

    liked			  boolean not null,
    viewed            boolean not null,

    time integer
);

create function mark_reaction_as_viewed(_user_id integer) returns boolean
    language plpgsql
as
$$
    begin
        if user_is_exist(_user_id) then
            update reactions set viewed = TRUE, time = extract(epoch from now()) where rated_id = _user_id
                                                                               and time = 0
                                                                                   and viewed = FALSE
                                                                                   and initiator_is_author = FALSE;
            if found then
                return True;
            end if;
        end if;
        return False;
    end;
$$;

create function create_ads(_user_id integer, text_ads text, link text, media text, token text, group_id integer, duration integer) returns boolean
    language plpgsql
as
$$
    begin
        if user_is_exist(_user_id) then
            update reactions set viewed = TRUE, liked = TRUE where rated_id = _user_id
                                                                               and liked = FALSE;
            if found then
                return True;
            end if;
        end if;
        return False;
    end;
$$;

create function respond_to_reaction(_user_id integer) returns boolean
    language plpgsql
as
$$
    begin
        if user_is_exist(_user_id) then
            update reactions set viewed = TRUE, liked = TRUE where rated_id = _user_id
                                                                               and liked = FALSE;
            if found then
                return True;
            end if;
        end if;
        return False;
    end;
$$;

create function create_user(_user_id integer,
                            user_name text,
                            user_sex integer,
                            _city text) returns boolean
    language plpgsql
as
$$
    begin
        if not user_is_exist(_user_id) then
            insert into messages(user_id, message_id, reaction_id) values (_user_id, 0, 0);
            insert into wait_actions(user_id, action) values(_user_id, '');
            insert into users(user_id, first_name, city, sex, search_sex,
                              is_hide) values (_user_id, user_name, _city, user_sex, 0, true);
            return True;
        end if;
        return False;
    end;
$$;



create function next_reaction(_user_id integer, period_sec integer) returns integer
    language plpgsql
as
$$
    declare
        max_id_user_reaction int;
        id_user_reaction int;
        id_next_user int;
        search_s int;
        user_id_next_user int;
        c_time integer;

        ads_id int;
        reaction_time int;
        ads_viewed boolean;
        ads_duration integer;
    begin
        if user_is_exist(_user_id) then
            c_time = extract(epoch from now());
            select t2.rated_id, t2.time, t2.viewed into ads_id, reaction_time, ads_viewed from reactions as t2 where t2.id = (
                select max(t1.id) from reactions as t1 where t1.initiator_id = _user_id and t1.is_ads = TRUE
                );
            if FOUND and ads_viewed and c_time - reaction_time > period_sec then
                select t1.id, t1.duration into ads_id, ads_duration from ads as t1 where t1.id > ads_id;
                    if not FOUND then
                        select t1.id, t1.duration into ads_id, ads_duration from ads as t1 where t1.id > 0;
                        if c_time - reaction_time > ads_duration then
                            update ads set is_hide = TRUE where id = ads_id;
                            select t1.id, t1.duration into ads_id, ads_duration from ads as t1 where t1.id > ads_id;
                        end if;
                    else
                        if c_time - reaction_time > ads_duration then
                            update ads set is_hide = TRUE where id = ads_id;
                        end if;
                    end if;
                    return create_reaction(_user_id, ads_id, TRUE);
            elsif FOUND and not ads_viewed then
               return ads_id;
            else
                select t2.search_sex into search_s from users as t2 where (t2.user_id = _user_id);
                    if found then
                        select max(t2.id) into max_id_user_reaction from reactions as t2 where ((t2.initiator_id = _user_id)
                            and (t2.rated_id > 0) and (t2.is_ads = FALSE));
                        if max_id_user_reaction is null then
                            max_id_user_reaction = 0;
                        end if;
                        select t2.rated_id into id_user_reaction from reactions as t2 where (t2.id = max_id_user_reaction);
                        select t2.id into id_next_user from users as t2 where (t2.user_id = id_user_reaction);
                        if search_s > 0 then
                            select t2.user_id into user_id_next_user from users as t2 where((t2.id > id_next_user) and
                                                                                            (t2.sex = search_s) and
                                                                                            (t2.user_id != _user_id));
                        else
                            select t2.user_id into user_id_next_user from users as t2 where((t2.id > id_next_user) and
                                                                                            (t2.user_id != _user_id));
                        end if;
                    end if;
                    return create_reaction(_user_id, user_id_next_user, FALSE);
            end if;
        end if;
        return 0;
    end;
$$;

create function remove_user(_user_id integer) returns boolean
    language plpgsql
as
$$
        begin
            delete from users where (user_id = _user_id);
            if found then
                delete from wait_actions where (user_id = _user_id);
                delete from messages where (user_id = _user_id);
                delete from reactions where (initiator_id = _user_id or
                                             rated_id = _user_id);
                return True;
            end if;
            return False;
        end;
$$;

create function user_is_exist(_user_id integer) returns boolean
    language plpgsql
as $$
    begin
        return (select t2.id from users as t2 where (t2.user_id = _user_id)) is not null;
    end;
$$;

create function activate_profile(_user_id integer) returns boolean
    language plpgsql
as $$
    begin
        update users set is_hide = FALSE where (user_id = _user_id);
        return found;
    end;
$$;

create function deactivate_profile(_user_id integer) returns boolean
    language plpgsql
as $$
    begin
        update users set is_hide = TRUE where (user_id = _user_id);
        return FOUND;
    end;
$$;

create function change_image(_user_id integer, image_url text) returns boolean
    language plpgsql
as $$
    begin
        update users set media = image_url where (user_id = _user_id);
        if FOUND then
            perform change_action(_user_id, '');
            return true;
        end if;
        return false;
    end;
$$;

create function create_reaction(initiator_user_id integer, rated_user_id integer, reaction_is_ads boolean) returns integer
    language plpgsql
as $$
    declare
        ret integer;
        t_time integer;
    begin
        if user_is_exist(initiator_user_id) then
            t_time = extract(epoch from now());
            insert into reactions(initiator_id, rated_id, liked, initiator_is_author, is_ads, viewed, time)
            values(initiator_user_id, rated_user_id, FALSE, TRUE, reaction_is_ads, FALSE, t_time) returning id into ret;
        end if;
        return ret;
    end;
$$;

create function get_user_id_next_reaction(_user_id integer) returns integer
    language plpgsql
as $$
    declare
        initiator_id int;
    begin
        if user_is_exist(_user_id) then
            select t2.initiator_id into initiator_id from reactions as t2 where (
                    t2.rated_id = _user_id and t2.liked = TRUE and t2.viewed = FALSE and t2.time > 0
                );
        end if;
        return initiator_id;
    end;
$$;

create function change_action(_user_id integer, action_name text) returns boolean
    language plpgsql
as $$
    begin
        if user_is_exist(_user_id) then
            update wait_actions set action = action_name where (user_id = _user_id);
            return TRUE;
        end if;
        return FALSE;
    end;
$$;

create function change_age(_user_id integer, user_years integer) returns boolean
    language plpgsql
as $$
    begin
        update users set age = user_years where (user_id = _user_id);
        if FOUND then
            perform change_action(_user_id, '');
            return true;
        end if;
        return false;
    end;
$$;

create function change_user_about(_user_id integer, user_about text) returns boolean
    language plpgsql
as $$
    begin
        update users set about = user_about where (user_id = _user_id);
        if FOUND then
            perform change_action(_user_id, '');
            return true;
        end if;
        return false;
    end;
$$;

create function change_location(_user_id integer, user_coordinates text) returns boolean
    language plpgsql
as $$
    begin
        update users set coordinates = user_coordinates where (user_id = _user_id);
        if FOUND then
            perform change_action(_user_id, '');
            return true;
        end if;
        return false;
    end;
$$;

create function get_action(_user_id integer) returns text
    language plpgsql
as $$
    declare
        fun_name text;
    begin
        if user_is_exist(_user_id) then
            select t2.action into fun_name from wait_actions as t2 where (t2.user_id = _user_id);
        end if;
        return fun_name;
    end;
$$;

create function get_first_name(_user_id integer) returns text
    language plpgsql
as $$
    declare
        first_name text;
    begin
        select t2.first_name into first_name from users as t2 where (t2.user_id = _user_id);
        return first_name;
    end;
$$;

create function change_search_sex(_user_id integer, user_search_sex integer) returns boolean
    language plpgsql
as $$
    begin
        update users set search_sex = user_search_sex where (user_id = _user_id);
        if FOUND then
            perform change_action(_user_id, '');
            return true;
        end if;
        return false;
    end
$$;

create function count_new_reactions(_user_id integer) returns integer
    language plpgsql
as $$
    declare
        count_reactions integer;
    begin
        if user_is_exist(_user_id) then
            select count(t2.id)into count_reactions from reactions as t2 where (
                t2.rated_id = _user_id and t2.liked = TRUE and t2.viewed = FALSE and t2.time > 0);
        end if;
        if count_reactions is null then
            count_reactions = 0;
        end if;
        return count_reactions;
    end
$$;

