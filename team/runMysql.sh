sqlun=root
export sqlun
sqlpw=root
export sqlpw
testdb=mysql
export testdb
back_end_idx=0
export back_end_idx
echo $sqlun
echo $sqlpw
echo $testdb
echo $back_end_idx
java -Xmx1024m -jar FrontEnd.jar
