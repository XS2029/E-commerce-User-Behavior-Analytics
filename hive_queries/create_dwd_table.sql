CREATE TABLE dwd_user_action AS
SELECT
    get_json_object(log, '$.timestamp') as ts,
    get_json_object(log, '$.user_id') as user_id,
    get_json_object(log, '$.action') as action,
    get_json_object(log, '$.product_id') as product_id,
    get_json_object(log, '$.price') as price
FROM ods_user_log;
