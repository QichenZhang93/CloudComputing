sqlun=root
export sqlun
sqlpw=root
export sqlpw
testdb=hbase
export testdb
echo $sqlun
echo $sqlpw
echo $testdb
java -Xmx1024m -jar FrontEnd.jar
