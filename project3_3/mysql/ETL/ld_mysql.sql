load data local infile '~/Documents/15619/project3_3/users.csv'
into table users
charset 'UTF8'
columns terminated by ','
lines terminated by '\n';

load data local infile '~/Documents/15619/project3_3/userinfo.csv'
into table userinfo
charset 'UTF8'
columns terminated by ','
lines terminated by '\n';