/usr/local/sqoop/bin/sqoop import --connect jdbc:mysql://127.0.0.1/project --username root --password Hadoop2015 --table vw_GPPracElderlyPop --hbase-table GPPracTypeDemog --column-family cf1 --hbase-row-key practype -m 1 

