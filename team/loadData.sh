for i in `seq 0 9`; do
curl http://s3.amazonaws.com/ccteambucket2/improve_q2_mysql/part-0000$i > ./part-0000$i
mv part-0000$i twitter
mysqlimport --lines-terminated-by '\n' --password=root --user=root --local --fields-terminated-by '\t\t\t' ccteam ./twitter
rm twitter
done


for i in `seq 10 60`; do
curl http://s3.amazonaws.com/ccteambucket2/improve_q2_mysql/part-000$i > ./part-000$i
mv part-000$i twitter
mysqlimport --lines-terminated-by '\n' --password=root --user=root --local --fields-terminated-by '\t\t\t' ccteam ./twitter
rm twitter
done

mysql -uroot -proot ccteam < creatIndex.sql

## load Q4 data TODO: