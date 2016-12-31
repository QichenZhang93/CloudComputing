CREATE DATABASE ccteam;

USE ccteam;

CREATE TABLE twitter (
    uid bigint unsigned primary key,
    tweets mediumtext,
    Scores text,
    Censortext mediumtext
)
DEFAULT CHARSET=utf8mb4 ENGINE = MYISAM
PARTITION BY HASH(uid)
PARTITIONS 10;

CREATE TABLE twitter_q3 (
    uid bigint unsigned primary key,
    tweets mediumtext
)
DEFAULT CHARSET=utf8mb4 ENGINE = MYISAM
PARTITION BY HASH(uid)
PARTITIONS 10;

INSERT twitter_q3 SELECT uid, tweets FROM twitter;

CREATE TABLE shard (
    tid bigint unsigned,
    Uid bigint unsigned,
    U_name varchar(255),
    Created_at varchar(255),
    Hashtags text,
    T_text text,
    id bigint AUTO_INCREMENT primary key
)
DEFAULT CHARSET=utf8mb4