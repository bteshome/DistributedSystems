

create table rules(
    api varchar(100) not null,
    is_per_client bit not null,
    granularity varchar(25) not null
    threshold int not null,
)

insert into rules
values ('*', 1, 'MINUTE', 3)
