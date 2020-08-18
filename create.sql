create table if not exists wait_actions
(
    user_id integer not null,
    action  text    not null
);

create table if not exists users
(
    id                serial,

    user_id           integer not null,
    first_name        text    not null,
    age               integer,
    city              text,
    about             text,
    media             text,
    coordinates       text,
    sex               integer not null,
    search_sex        integer not null,
    search_before_age integer not null,
    search_after_age  integer not null,
    is_hide           boolean not null
);

create table if not exists ads
(
    id          serial,

    message_id  integer not null,
    group_id    integer not null,
    token       text    not null,
    is_hide     boolean not null,

    text        text,
    media       text,
    link        text,

    duration    integer not null,
    create_time integer not null
);

create table if not exists messages
(
    user_id     integer not null,
    message_id  integer not null,
    reaction_id integer not null
);

create table if not exists reactions
(
    id                  serial,

    message_id          integer,
    rated_message_id    integer,
    initiator_id        integer not null,
    rated_id            integer not null,

    is_ads              boolean not null,
    initiator_is_author boolean not null,

    liked               boolean,
    disliked            boolean,
    viewed              boolean not null,

    time                integer
);

create or replace function last_reaction_id(_user_id integer) returns integer
    language plpgsql
as
$$
declare
    reaction_id integer;
begin
    select max(t2.id) into reaction_id from reactions as t2 where t2.initiator_id = _user_id;
    return reaction_id;
end;
$$;

create function mark_reaction_as_viewed(_user_id integer, _message_id integer) returns boolean
    language plpgsql
as
$$
begin
    update reactions
    set viewed = TRUE
    where (
                initiator_is_author = TRUE and
                (
                        message_id = _message_id and
                        initiator_id = _user_id and
                        is_ads = TRUE
                    )
            or (
                        rated_message_id = _message_id and
                        rated_id = _user_id and
                        is_ads = FALSE
                    ))
      and viewed = FALSE;
    return true;
end;
$$;


create or replace function create_ads(_user_id integer, _message_id integer, _text text, _link text, _media text,
                                      _token text,
                                      _group_id integer, _duration integer) returns boolean
    language plpgsql
as
$$
declare
    c_time integer;
begin
    if user_is_exist(_user_id) then
        c_time = extract(epoch from now());
        insert into ads(group_id, message_id, token, is_hide, text, media, link, duration, create_time)
        values (_group_id, _message_id, _token, TRUE, _text, _media, _link, _duration, c_time);
        perform change_action(_user_id, '');
        return True;
    end if;
    return False;
end;
$$;

create or replace function activate_ads(_user_id integer) returns boolean
    language plpgsql
as
$$
begin
    if user_is_exist(_user_id) then
        update reactions
        set viewed = TRUE,
            liked  = TRUE
        where rated_id = _user_id
          and liked = FALSE;
        if found then
            return True;
        end if;
    end if;
    return False;
end;
$$;

create or replace function respond_to_reaction(_user_id integer) returns boolean
    language plpgsql
as
$$
begin
    if user_is_exist(_user_id) then
        update reactions
        set viewed = TRUE,
            liked  = TRUE
        where rated_id = _user_id
          and liked = FALSE;
        if found then
            return True;
        end if;
    end if;
    return False;
end;
$$;

create or replace function create_user(_user_id integer,
                                       user_name text,
                                       user_sex integer,
                                       _city text) returns boolean
    language plpgsql
as
$$
begin
    if not user_is_exist(_user_id) then
        insert into messages(user_id, message_id, reaction_id) values (_user_id, 0, 0);
        insert into wait_actions(user_id, action) values (_user_id, '');
        insert into users(user_id, first_name, city, sex, search_sex, search_before_age, search_after_age,
                          is_hide)
        values (_user_id, user_name, _city, user_sex, 0, 0, 100, true);
        return True;
    end if;
    return False;
end;
$$;



create or replace function next_reaction(_user_id integer, show_interval integer) returns integer
    language plpgsql
as
$$
declare
    _rated_id            int;
    _is_ads              boolean;
    _count_users_for_ads integer;
begin
    if user_is_exist(_user_id) then
        update users set is_hide = FALSE where user_id = _user_id;
        select count(t2.id)
        into _count_users_for_ads
        from reactions as t2
        where t2.initiator_id = _user_id
          and t2.initiator_is_author = TRUE
          and t2.is_ads = FALSE;
        if FOUND and _count_users_for_ads >= 2 then
            _rated_id = select_ads(_user_id, show_interval);
            _is_ads = True;
            if _rated_id is null then
                _rated_id = next_user(_user_id);
                _is_ads = False;
            end if;
        else
            _rated_id = next_user(_user_id);
            _is_ads = False;
        end if;
        if _rated_id is not null then
            return create_reaction(_user_id, _rated_id, TRUE, _is_ads);
        end if;
    end if;
    return null;
end;
$$;

create or replace function select_ads(_user_id integer, show_interval integer) returns integer
    language plpgsql
as
$$
declare
    c_time            integer;
    ads_id            integer;
    reaction_rated_id integer;
    reaction_time     integer;
    ads_viewed        boolean;
    ads_duration      integer;
    t                 integer;
    ads_created_time  integer;
begin
    c_time = extract(epoch from now());
    select t2.id,
           t2.rated_id,
           t2.time,
           t2.viewed
    into
        ads_id,
        reaction_rated_id,
        reaction_time,
        ads_viewed
    from reactions as t2
    where t2.id = (
        select max(t1.id)
        from reactions as t1
        where t1.initiator_id = _user_id
          and t1.is_ads = TRUE
    );
    if reaction_time is null then
        t = 0;
    else
        t = c_time - reaction_time;
    end if;
    if not found then
        reaction_rated_id = next_ads(0);
    else
        select t2.duration, t2.create_time
        into ads_duration, ads_created_time
        from ads as t2
        where t2.id = reaction_rated_id
          and t2.is_hide = FALSE
          and t2.create_time + t2.duration > c_time;
        if found then
            if ads_viewed then
                if t > show_interval then
                    reaction_rated_id = next_ads(reaction_rated_id);
                else
                    return null;
                end if;
            else
                return null;
            end if;
        else
            update ads set is_hide = TRUE where id = reaction_rated_id;
            if t > show_interval then
                reaction_rated_id = next_ads(reaction_rated_id);
            else
                return null;
            end if;
        end if;
    end if;
    return reaction_rated_id;
end;
$$;

create or replace function next_user(_user_id integer) returns integer
    language plpgsql
as
$$
declare
    search_s     integer;
    id_next_user integer;
    before_age   integer;
    after_age    integer;
begin
    select t2.search_sex,
           t2.search_before_age,
           t2.search_after_age
    into
        search_s,
        before_age,
        after_age
    from users as t2
    where (t2.user_id = _user_id);

    if FOUND then
        if search_s > 0 then
            select t2.user_id
            into id_next_user
            from users as t2
            where (t2.user_id not in (
                select t3.rated_id
                from reactions as t3
                where (t3.initiator_id = _user_id and
                       t3.rated_id > 0 and
                       t3.is_ads = FALSE)
            ) and
                   t2.user_id != _user_id and
                   t2.sex = search_s and
                   t2.is_hide is FALSE and
                   t2.age >= before_age and
                   t2.age <= after_age)
            limit 1;
        else
            select t2.user_id
            into id_next_user
            from users as t2
            where (t2.user_id not in (
                select t3.rated_id
                from reactions as t3
                where (t3.initiator_id = _user_id and
                       t3.rated_id > 0 and
                       t3.is_ads = FALSE)
            ) and
                   t2.user_id != _user_id and
                   t2.is_hide is FALSE and
                   t2.age >= before_age and
                   t2.age <= after_age)
            limit 1;
        end if;
    end if;
    return id_next_user;
end;
$$;

create or replace function next_ads(ads_id integer) returns integer
    language plpgsql
as
$$
declare
    ret_ads_id integer;
    c_time     integer;
begin
    if ads_id is null then
        ads_id = 0;
    end if;
    c_time = extract(epoch from now());
    select t2.id
    into ret_ads_id
    from ads as t2
    where t2.id > ads_id
      and t2.create_time + t2.duration > c_time
      and t2.is_hide = FALSE;
    if not found then
        select t2.id
        into ret_ads_id
        from ads as t2
        where t2.id > 0
          and t2.create_time + t2.duration > c_time
          and t2.is_hide = FALSE;
    end if;
    return ret_ads_id;
end;
$$;

create or replace function remove_user(_user_id integer) returns boolean
    language plpgsql
as
$$
begin
    delete from users where (user_id = _user_id);
    if found then
        delete from wait_actions where (user_id = _user_id);
        delete from messages where (user_id = _user_id);
        delete
        from reactions
        where (initiator_id = _user_id or
               rated_id = _user_id);
        return True;
    end if;
    return False;
end;
$$;

create or replace function user_is_exist(_user_id integer) returns boolean
    language plpgsql
as
$$
begin
    return (select t2.id from users as t2 where (t2.user_id = _user_id)) is not null;
end;
$$;

create or replace function activate_profile(_user_id integer) returns boolean
    language plpgsql
as
$$
begin
    update users set is_hide = FALSE where (user_id = _user_id);
    return found;
end;
$$;

create or replace function deactivate_profile(_user_id integer) returns boolean
    language plpgsql
as
$$
begin
    update users set is_hide = TRUE where (user_id = _user_id);
    return FOUND;
end;
$$;

create or replace function last_created_reaction_for_dislike(_user_id integer) returns boolean
    language plpgsql
as
$$
declare
    last_reaction integer;
begin
    if user_is_exist(_user_id) then
        select max(t2.id)
        into last_reaction
        from reactions as t2
        where t2.is_ads = FALSE
          and t2.rated_message_id is null
          and t2.initiator_is_author = TRUE
          and t2.liked is null
          and t2.disliked is null
          and t2.viewed = FALSE
          and t2.initiator_id = _user_id;
        if found then
            update reactions set disliked = TRUE where id = last_reaction;
            return TRUE;
        end if;
    end if;
    return FALSE;
end;
$$;

create or replace function change_image(_user_id integer, image_url text) returns boolean
    language plpgsql
as
$$
begin
    update users set media = image_url where (user_id = _user_id);
    if FOUND then
        perform change_action(_user_id, '');
        return true;
    end if;
    return false;
end;
$$;

create or replace function create_reaction(initiator_user_id integer, rated_user_id integer,
                                           _initiator_is_author boolean, reaction_is_ads boolean) returns integer
    language plpgsql
as
$$
declare
    ret    integer;
    t_time integer;
begin
    if user_is_exist(initiator_user_id) then
        t_time = extract(epoch from now());
        insert into reactions(initiator_id, rated_id, initiator_is_author, is_ads, viewed, time)
        values (initiator_user_id, rated_user_id, _initiator_is_author, reaction_is_ads, FALSE, t_time)
        returning id into ret;
    end if;
    return ret;
end;
$$;

create or replace function get_id_next_reaction(_user_id integer) returns integer
    language plpgsql
as
$$
declare
    initiator_id int;
    _id          integer;
begin
    if user_is_exist(_user_id) then
        select t2.id,
               t2.initiator_id
        into _id,
            initiator_id
        from reactions as t2
        where (
                      t2.rated_id = _user_id and
                      t2.liked = TRUE and
                      t2.viewed is FALSE
                  )
        limit 1;
        if FOUND then
            perform create_reaction(_user_id, initiator_id, FALSE, FALSE);
        end if;
    end if;
    return _id;
end;
$$;

create or replace function last_created_reaction_for_like(_user_id integer) returns integer
    language plpgsql
as
$$
declare
    last_reaction integer;
    _rated_id integer;
begin
    if user_is_exist(_user_id) then
        select max(t2.id)
        into last_reaction
        from reactions as t2
        where t2.is_ads = FALSE
          and t2.liked is null
          and t2.disliked is null
          and t2.initiator_id = _user_id;
        if found then
            select t3.rated_id into _rated_id
                from reactions as t3
                where t3.id = last_reaction;
            update reactions set liked = TRUE where id = last_reaction;
            perform t2.id
            from reactions as t2
            where t2.initiator_id = _rated_id
              and t2.rated_id = _user_id
              and t2.liked = TRUE;
            if found then
                return last_reaction;
            end if;
        end if;
    end if;
    return null;
end;
$$;

create or replace function change_action(_user_id integer, action_name text) returns boolean
    language plpgsql
as
$$
begin
    if user_is_exist(_user_id) then
        update wait_actions set action = action_name where (user_id = _user_id);
        return TRUE;
    end if;
    return FALSE;
end;
$$;

create or replace function reset_ads(ads_id integer) returns boolean
    language plpgsql
as
$$
declare
    t_time integer;
begin
    t_time = extract(epoch from now());
    update ads set create_time = t_time where (id = ads_id);
    return true;
end;
$$;

create or replace function change_age(_user_id integer, user_years integer) returns boolean
    language plpgsql
as
$$
begin
    update users set age = user_years where (user_id = _user_id);
    if FOUND then
        perform change_action(_user_id, '');
        return true;
    end if;
    return false;
end;
$$;

create or replace function change_user_about(_user_id integer, user_about text) returns boolean
    language plpgsql
as
$$
begin
    update users set about = user_about where (user_id = _user_id);
    if FOUND then
        perform change_action(_user_id, '');
        return true;
    end if;
    return false;
end;
$$;

create or replace function change_location(_user_id integer, user_coordinates text) returns boolean
    language plpgsql
as
$$
begin
    update users set coordinates = user_coordinates where (user_id = _user_id);
    if FOUND then
        perform change_action(_user_id, '');
        return true;
    end if;
    return false;
end;
$$;

create or replace function get_action(_user_id integer) returns text
    language plpgsql
as
$$
declare
    fun_name text;
begin
    if user_is_exist(_user_id) then
        select t2.action into fun_name from wait_actions as t2 where (t2.user_id = _user_id);
    end if;
    return fun_name;
end;
$$;

create or replace function get_first_name(_user_id integer) returns text
    language plpgsql
as
$$
declare
    first_name text;
begin
    select t2.first_name into first_name from users as t2 where (t2.user_id = _user_id);
    return first_name;
end;
$$;

create or replace function change_search_sex(_user_id integer, user_search_sex integer) returns boolean
    language plpgsql
as
$$
begin
    update users set search_sex = user_search_sex where (user_id = _user_id);
    if FOUND then
        perform change_action(_user_id, '');
        return true;
    end if;
    return false;
end
$$;

create or replace function count_new_reactions(_user_id integer) returns integer
    language plpgsql
as
$$
declare
    count_reactions integer;
begin
    if user_is_exist(_user_id) then
        select count(t2.id)
        into count_reactions
        from reactions as t2
        where (
                      t2.rated_id = _user_id and
                      t2.liked = TRUE and
                      t2.viewed = FALSE
                  );
        if found then
            return count_reactions;
        end if;
    end if;
    return 0;
end
$$;

