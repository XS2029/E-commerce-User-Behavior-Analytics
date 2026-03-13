import org.apache.spark.sql.functions._
import java.util.Properties

val factDF = spark.sql("select * from dwd_user_action")
val dimDF = spark.sql("select * from dim_user_hbase")

val purchaseDF = factDF.filter(col("action") === "purchase")
purchaseDF.createOrReplaceTempView("purchases")

val rfmAggDF = purchaseDF.groupBy("user_id")
  .agg(
    max("ts").as("last_purchase_time"),
    count("product_id").as("frequency"),
    sum("price").as("monetary")
  )
rfmAggDF.createOrReplaceTempView("rfm_agg")

val rfmScored = spark.sql("""
  select 
      user_id,
      case 
          when frequency >= 10 then 5
          when frequency >= 7 then 4
          when frequency >= 4 then 3
          when frequency >= 2 then 2
          else 1
      end as f_score,
      case 
          when monetary >= 1000 then 5
          when monetary >= 500 then 4
          when monetary >= 200 then 3
          when monetary >= 50 then 2
          else 1
      end as m_score,
      case 
          when (unix_timestamp() - last_purchase_time) < 7*24*3600 then 5
          when (unix_timestamp() - last_purchase_time) < 30*24*3600 then 4
          when (unix_timestamp() - last_purchase_time) < 90*24*3600 then 3
          when (unix_timestamp() - last_purchase_time) < 180*24*3600 then 2
          else 1
      end as r_score
  from rfm_agg
""")
rfmScored.createOrReplaceTempView("rfm_scored")

val rfmFinal = spark.sql("""
  select 
      user_id,
      r_score,
      f_score,
      m_score,
      (r_score + f_score + m_score) as total_score,
      case 
          when (r_score + f_score + m_score) >= 13 then '高价值'
          when (r_score + f_score + m_score) >= 8 then '中价值'
          else '低价值'
      end as user_segment
  from rfm_scored
""")

rfmFinal.show(20)

val jdbcUrl = "jdbc:mysql://localhost:3306/metastore?useSSL=false"
val props = new Properties()
props.setProperty("user", "root")
props.setProperty("password", "344276714")
props.setProperty("driver", "com.mysql.cj.jdbc.Driver")

rfmFinal.write.mode("overwrite").jdbc(jdbcUrl, "user_rfm", props)
