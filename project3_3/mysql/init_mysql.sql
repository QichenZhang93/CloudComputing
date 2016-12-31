create table users (
	id int primary key,
    pw text
);

create table userinfo (
	id int primary key references users(id) on delete cascade,
    uname text,
    profile_img_url text
);