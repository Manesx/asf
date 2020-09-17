drop table if exists ads, messages, reactions, users, wait_actions cascade;
drop view if exists empty_reactions, liked_reactions, profile_reactions, reactions_ads;

create table if not exists users
(
    id                serial,

    user_id           integer primary key,
    first_name        text    not null,
    age               integer default 0,
    city              text    not null,
    about             text,
    media             text,
    locationLatitude  real    not null,
    locationLongitude real    not null,
    sex               integer not null,
    search_sex        integer default 0,
    search_before_age integer default 1,
    search_after_age  integer default 100,
    is_hide           boolean default TRUE
);

create table if not exists ads
(
    id          serial,

    message_id  integer not null,
    is_hide     boolean default TRUE,

    description text    not null,
    media       text    not null,
    link        text    not null,

    duration    integer not null,
    create_time integer default extract(epoch from now())
);

create table if not exists reactions
(
    id           serial,

    message_id   integer,
    initiator_id integer not null,
    rated_id     integer not null,

    is_ads       boolean not null,

    liked        boolean default FALSE,
    disliked     boolean default FALSE,
    viewed       boolean default FALSE,

    time         integer default extract(epoch from now()),
    foreign key (initiator_id) references users (user_id) on delete cascade,
    foreign key (rated_id) references users (user_id) on delete cascade
);

create table if not exists wait_actions
(
    user_id integer,
    action  text,
    foreign key (user_id) references users (user_id) on delete cascade
);

create view reactions_ads as
select t2.id,
       t2.initiator_id as author_id,
       t2.rated_id     as ads_id,
       t2.viewed,
       t2.time
from reactions as t2
where t2.is_ads = TRUE;

create view profile_reactions as
select t2.id,
       t2.message_id,
       t2.initiator_id,
       t2.rated_id,
       t2.liked,
       t2.disliked,
       t2.viewed,
       t2.time
from reactions as t2
where t2.is_ads = FALSE;

create view empty_reactions as
select t2.id,
       t2.message_id,
       t2.initiator_id,
       t2.rated_id,
       t2.viewed,
       t2.time
from profile_reactions as t2
where t2.liked is FALSE
  and t2.disliked is FALSE;

create view liked_reactions as
select t2.id,
       t2.initiator_id,
       t2.rated_id,
       t2.viewed,
       t2.time
from profile_reactions as t2
where t2.liked is TRUE
  and t2.disliked is FALSE
  and not exists(select t3.id
                 from profile_reactions as t3
                 where t3.rated_id = t2.initiator_id
                   and t3.initiator_id = t2.rated_id);



create table if not exists messages
(
    user_id     integer not null,
    message_id  integer,
    reaction_id integer,
    foreign key (user_id) references users (user_id) on delete cascade
);

create or replace function haversine(latitude1 real, longitude1 real, latitude2 real, longitude2 real) returns real
    language plpgsql
as
$$
declare
    earth_r constant integer = 6371302;
    a                real;
begin
    a = pow(sin(radians(latitude2 - latitude1) / 2), 2) +
        cos(radians(latitude1)) * cos(radians(latitude2)) * pow(sin(radians(longitude2 - longitude1) / 2), 2);
    return 2 * earth_r * atan2(sqrt(a), sqrt(1 - a));
end;
$$;

create function viewed_reaction(_user_id integer, _message_id integer) returns boolean
    language plpgsql
as
$$
begin
    update reactions as t2
    set viewed = TRUE
    where t2.message_id = _message_id
      and t2.initiator_id = _user_id
      and t2.viewed = FALSE;
    return FOUND;
end;
$$;

create or replace function create_ads(_user_id integer,
                                      _message_id integer,
                                      _text text,
                                      _link text,
                                      _media text,
                                      _duration integer) returns boolean
    language plpgsql
as
$$
begin
    if user_is_exist(_user_id) then
        insert into ads(message_id,
                        description,
                        media,
                        link,
                        duration)
        values (_message_id,
                _text,
                _media,
                _link,
                _duration);
        perform clear_action(_user_id);
        return True;
    end if;
    return False;
end;
$$;

create or replace function activate_ads(ads_id integer) returns boolean
    language plpgsql
as
$$
begin
    update ads as t2
    set is_hide = FALSE
    where t2.id = ads_id;
    return FOUND;
end;
$$;

create or replace function deactivate_ads(ads_id integer) returns boolean
    language plpgsql
as
$$
begin
    update ads as t2
    set is_hide = TRUE
    where t2.id = ads_id;
    return FOUND;
end;
$$;

create function create_user(_user_id integer, user_name text, user_sex integer, _city text, _locationlatitude real,
                            _locationlongitude real) returns boolean
    language plpgsql
as
$$
begin
    if not user_is_exist(_user_id) then
        insert into users(user_id,
                          first_name,
                          city,
                          sex,
                          locationLatitude,
                          locationLongitude)
        values (_user_id,
                user_name,
                _city,
                user_sex,
                _locationLatitude,
                _locationLongitude);
        insert into messages(user_id)
        values (_user_id);
        insert into wait_actions(user_id)
        values (_user_id);
        return True;
    end if;
    return False;
end;
$$;



create or replace function next_reaction(_user_id integer, show_interval integer)
    returns
        table
        (
            _is_ads      boolean,
            _reaction_id integer,
            ads_id       integer,
            description  text,
            link         text,
            user_id      integer,
            first_name   text,
            age          integer,
            city         text,
            sex          integer,
            about        text,
            media        text,
            distance     real
        )
    language plpgsql
as
$$
declare
    _rated_id            integer;
    _reaction_id         integer;
    _is_ads              boolean = FALSE;
    _count_users_for_ads integer;
    u_latitude           real;
    u_longitude          real;
begin
    if is_fulled(_user_id) then
        update users as t2
        set is_hide = FALSE
        where t2.user_id = _user_id
        returning t2.locationlatitude, t2.locationlongitude into u_latitude, u_longitude;
        select count(t2.id)
        into _count_users_for_ads
        from profile_reactions as t2
        where t2.initiator_id = _user_id;
        if FOUND and _count_users_for_ads >= 2 then
            _rated_id = select_ads(_user_id, show_interval);
            if _rated_id is not null then
               _is_ads = TRUE;
            end if;
        end if;
        if _rated_id is null then
            select t3.id,
                   t3.rated_id
            into _reaction_id,
                _rated_id
            from empty_reactions as t3
            where t3.initiator_id = _user_id
            limit 1;
            if found then
                update reactions as t2
                set message_id = null::integer,
                    time       = extract(epoch from now())::integer,
                    viewed     = FALSE
                where t2.id = _reaction_id;
            end if;
        end if;
        if _rated_id is null then
            _rated_id = next_user(_user_id);
        end if;
        if _rated_id is not null then
            if _reaction_id is null then
                _reaction_id = create_reaction(_user_id, _rated_id, _is_ads);
            end if;
            if _is_ads then
                return query select _is_ads,
                                    _reaction_id,
                                    t3.id         as ads_id,
                                    t3.description,
                                    t3.link,
                                    null::integer as user_id,
                                    null          as first_name,
                                    null::integer as age,
                                    null          as city,
                                    null::integer as sex,
                                    null          as about,
                                    t3.media,
                                    null::real    as distance
                             from ads as t3
                             where t3.id = _rated_id;
            else
                return query select _is_ads,
                                    _reaction_id,
                                    null::integer                   as ads_id,
                                    null                            as description,
                                    null                            as link,
                                    t3.user_id,
                                    t3.first_name,
                                    t3.age,
                                    t3.city,
                                    t3.sex,
                                    t3.about,
                                    t3.media,
                                    haversine(u_latitude,
                                              u_longitude,
                                              t3.locationLatitude,
                                              t3.locationLongitude) as distance
                             from users as t3
                             where t3.user_id = _rated_id;
            end if;
        end if;
    end if;
end;
$$;

create or replace function select_ads(_user_id integer, show_interval integer) returns integer
    language plpgsql
as
$$
declare
    c_time           integer;
    _ads_id          integer;
    _author_id       integer;
    reaction_time    integer;
    ads_viewed       boolean;
    ads_duration     integer;
    t                integer;
    ads_created_time integer;
begin
    c_time = extract(epoch from now());
    select t2.id,
           t2.author_id,
           t2.time,
           t2.viewed
    into
        _ads_id,
        _author_id,
        reaction_time,
        ads_viewed
    from reactions_ads as t2
    where t2.id = (
        select max(t1.id)
        from reactions_ads as t1
        where t1.author_id = _user_id
    );
    if reaction_time is null then
        t = 0;
    else
        t = c_time - reaction_time;
    end if;
    if not found then
        _author_id = next_ads(0);
    else
        select t2.duration, t2.create_time
        into ads_duration, ads_created_time
        from ads as t2
        where t2.id = _author_id
          and t2.is_hide = FALSE
          and t2.create_time + t2.duration > c_time;
        if found then
            if ads_viewed then
                if t > show_interval then
                    _author_id = next_ads(_author_id);
                else
                    return null;
                end if;
            else
                return null;
            end if;
        else
            update ads as t2 set is_hide = TRUE where t2.id = _author_id;
            if t > show_interval then
                _author_id = next_ads(_author_id);
            else
                return null;
            end if;
        end if;
    end if;
    return _author_id;
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
    user_age     integer;
    latitude     real;
    longitude    real;
    distance     real;
begin
    select t2.search_sex,
           t2.search_before_age,
           t2.search_after_age,
           t2.age,
           t2.locationLatitude,
           t2.locationLongitude
    into
        search_s,
        before_age,
        after_age,
        user_age,
        latitude,
        longitude
    from users as t2
    where t2.user_id = _user_id;
    if FOUND then
        select t3.initiator_id
        into id_next_user
        from liked_reactions as t3
        where t3.rated_id = _user_id
        limit 1;
        if not FOUND then
            if search_s > 0 then
                select t2.user_id,
                       haversine(t2.locationLatitude,
                                 t2.locationLongitude,
                                 latitude,
                                 longitude) as _distance
                into id_next_user,
                    distance
                from users as t2
                where (t2.user_id not in (
                    select t3.rated_id
                    from profile_reactions as t3
                    where t3.initiator_id = _user_id
                ) and
                       t2.user_id != _user_id and
                       t2.sex = search_s and
                       t2.is_hide is FALSE and
                       ((t2.age >= before_age and
                         t2.age <= after_age) or
                        (t2.search_before_age <= user_age and
                         t2.search_after_age >= user_age)))
                order by _distance
                limit 1;
            else
                select t2.user_id,
                       haversine(t2.locationLatitude,
                                 t2.locationLongitude,
                                 latitude,
                                 longitude) as _distance
                into id_next_user,
                    distance
                from users as t2
                where (t2.user_id not in (
                    select t3.rated_id
                    from profile_reactions as t3
                    where t3.initiator_id = _user_id
                ) and
                       t2.user_id != _user_id and
                       t2.is_hide is FALSE and
                       ((t2.age >= before_age and
                         t2.age <= after_age) or
                        (t2.search_before_age <= user_age and
                         t2.search_after_age >= user_age)))
                order by _distance
                limit 1;
            end if;
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

create function user_is_exist(_user_id integer) returns boolean
    language plpgsql
as
$$
begin
    return exists(select t2.id from users as t2 where t2.user_id = _user_id);
end;
$$;

create or replace function dislike_profile(_user_id integer) returns boolean
    language plpgsql
as
$$
begin
    update reactions as t2
    set disliked = TRUE
    where t2.id = (select max(t2.id)
                   from empty_reactions as t2
                   where initiator_id = _user_id
                     and message_id is not null);
    return FOUND;
end;
$$;

create or replace function change_image(_user_id integer, image_url text) returns boolean
    language plpgsql
as
$$
begin
    update users as t2 set media = image_url where t2.user_id = _user_id;
    if FOUND then
        perform clear_action(_user_id);
        return true;
    end if;
    return false;
end;
$$;

create or replace function create_reaction(u_id integer, p_id integer,
                                           reaction_is_ads boolean) returns integer
    language plpgsql
as
$$
declare
    ret integer;
begin
    insert into reactions(initiator_id,
                          rated_id,
                          is_ads)
    values (u_id,
            p_id,
            reaction_is_ads)
    returning id into ret;
    return ret;
end;
$$;

create or replace function like_profile(_user_id integer)
    returns
        table
        (
            is_respond   boolean,
            _reaction_id integer,
            user_id      integer,
            first_name   text,
            age          integer,
            city         text,
            sex          integer,
            about        text,
            media        text,
            distance     real
        )
    language plpgsql
as
$$
declare
    _rated_id    integer;
    _reaction_id integer;
    u_latitude   real;
    u_longitude  real;
begin
    select t1.locationLatitude, t1.locationLongitude
    into u_latitude, u_longitude
    from users as t1
    where t1.user_id = _user_id;
    update reactions as t2
    set liked = TRUE
    where t2.id = (select max(t2.id)
                   from empty_reactions as t2
                   where t2.initiator_id = _user_id
                     and t2.message_id is not null)
    returning id, rated_id into _reaction_id, _rated_id;
    if FOUND then
        return query select exists(select t2.id
                                   from profile_reactions as t2
                                   where t2.rated_id = _user_id
                                     and t2.initiator_id = _rated_id and
                                         t2.liked is TRUE) as is_respond,
                            _reaction_id,
                            t3.user_id,
                            t3.first_name,
                            t3.age,
                            t3.city,
                            t3.sex,
                            t3.about,
                            t3.media,
                            haversine(u_latitude,
                                      u_longitude,
                                      t3.locationLatitude,
                                      t3.locationLongitude)           as distance
                     from users as t3
                     where t3.user_id = _rated_id;
    end if;
end;
$$;

-- create or replace function follow_profile(_user_id integer)
--     returns
--         table
--         (
--             _reaction_id integer,
--             user_id      integer,
--             first_name   text,
--             age          integer,
--             city         text,
--             sex          integer,
--             about        text,
--             media        text,
--             distance     real
--         )
--     language plpgsql
-- as
-- $$
-- declare
--     _initiator_id integer;
--     _reaction_id  integer;
--     u_latitude    real;
--     u_longitude   real;
-- begin
--     if user_is_exist(_user_id) then
--         select t1.locationLatitude, t1.locationLongitude
--         into u_latitude, u_longitude
--         from users as t1
--         where t1.user_id = _user_id;
--         select t2.initiator_id
--         into _initiator_id
--         from liked_reactions as t2
--         where t2.rated_id = _user_id
--         limit 1;
--         if found then
--             _reaction_id = create_reaction(_user_id, _initiator_id, FALSE);
--             return query select _reaction_id,
--                                 t2.user_id,
--                                 t2.first_name,
--                                 t2.age,
--                                 t2.city,
--                                 t2.sex,
--                                 t2.about,
--                                 t2.media,
--                                 haversine(u_latitude,
--                                           u_longitude,
--                                           t2.locationLatitude,
--                                           t2.locationLongitude) as distance
--                          from users as t2
--                          where t2.user_id = _initiator_id;
--         end if;
--     end if;
-- end;
-- $$;

create or replace function change_action(_user_id integer, action_name text) returns boolean
    language plpgsql
as
$$
begin
    update wait_actions as t2 set action = action_name where t2.user_id = _user_id;
    return FOUND;
end;
$$;

create or replace function clear_action(_user_id integer) returns boolean
    language plpgsql
as
$$
begin
    update wait_actions as t2 set action = null where t2.user_id = _user_id;
    return FOUND;
end;
$$;

create or replace function reset_ads(ads_id integer) returns boolean
    language plpgsql
as
$$
begin
    update ads as t2 set create_time = extract(epoch from now()) where t2.id = ads_id;
    return FOUND;
end;
$$;

create or replace function change_age(_user_id integer, user_years integer) returns boolean
    language plpgsql
as
$$
begin
    update users as t2 set age = user_years where t2.user_id = _user_id;
    if FOUND then
        perform clear_action(_user_id);
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
    update users as t2 set about = user_about where t2.user_id = _user_id;
    if FOUND then
        perform clear_action(_user_id);
        return true;
    end if;
    return false;
end;
$$;

create or replace function change_location(_user_id integer, _city text, latitude real, longitude real) returns boolean
    language plpgsql
as
$$
begin
    update users as t2
    set locationLatitude  = latitude,
        locationLongitude = longitude,
        city              = _city
    where t2.user_id = _user_id;
    if FOUND then
        perform clear_action(_user_id);
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
    select t2.action into fun_name from wait_actions as t2 where t2.user_id = _user_id;
    return fun_name;
end;
$$;

create or replace function change_search_sex(_user_id integer, user_search_sex integer) returns boolean
    language plpgsql
as
$$
begin
    update users as t2 set search_sex = user_search_sex where t2.user_id = _user_id;
    if FOUND then
        perform clear_action(_user_id);
        return true;
    end if;
    return false;
end
$$;

create or replace function is_fulled(_user_id integer) returns boolean
    language plpgsql
as
$$
begin
    return exists(select t2.id
                  from users as t2
                  where t2.age > 0
                    and t2.media is not null
                    and t2.about is not null
                    and t2.user_id = _user_id);
end
$$;
