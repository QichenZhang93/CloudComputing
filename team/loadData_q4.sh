########### Use before live test to clean data
echo 'truncate shard' > clear_q4.sql
mysql -uroot -proot ccteam < clear_q4.sql
./loadData_q4.sh
########### Use before live test to clean data

## Use before live test to warm machine
echo 'select count(*) from shard' > warm.sql
mysql -uroot -proot ccteam < warm.sql
## Use before live test to warm machine

for i in `seq 0 9`; do
echo $i
curl http://s3.amazonaws.com/ccteambucket2/new_sharding_0/part-0000$i > ./part-0000$i
mv part-0000$i shard
mysqlimport --lines-terminated-by '\n' --password=root --user=root --local --fields-terminated-by '\t\t' ccteam ./shard
rm shard
done

mysql -uroot -proot ccteam < creatIndex_q4.sql