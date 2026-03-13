CREATE EXTERNAL TABLE dim_user_hbase(
    user_id string,
    gender string,
    age string
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key, info:gender, info:age")
TBLPROPERTIES ("hbase.table.name" = "dim_user");
