create table if not exists changes_per_minute (
    window_start    timestamp   not null,
    window_end      timestamp   not null,
    count           bigint      not null,
    primary key (window_start, window_end)
);

create table if not exists changes_by_kind (
    kind    text    primary key,
    count   bigint  not null
);

create table if not exists changes_by_actor_type (
    actor_type  text    primary key,
    count       bigint  not null
);