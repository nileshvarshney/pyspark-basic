from unittest import case
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,col, to_date
spark = SparkSession.builder.master("local").appName("event processing").getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

data = spark.read.json("datasets/source_event_data.json")
# df2 = data.select("user.id", "user.ip", "user.session_id","event_id","action","url", "timestamp")
# df2.show(10)
data.createOrReplaceTempView("source_event_raw")

spark.sql("""
    select event_id,action,user,
    from_unixtime(unix_timestamp(timestamp, 'dd/MM/yyyy HH:mm:ss'),'yyyy/MM/dd hh:mm:ss') timestamp,
    cleaned_url,
    split(cleaned_url,"/")[2]  url_index1,
    split(cleaned_url,"/")[3]  url_index2,
    split(cleaned_url,"/")[4]  url_index3
    from (\
            select *,
                case
                    when locate("://", url) = 0 then concat("https://",url)
                    else url
                end cleaned_url
            from source_event_raw
        ) tab
    """
).createOrReplaceTempView("event_master")

# df = spark.sql("select * from event_master")
# df.printSchema()

#test = spark.sql("select from_unixtime(unix_timestamp(timestamp, 'dd/MM/yyyy HH:mm:ss'),'yyyy/MM/dd hh:mm:ss') t from event_master") 
activities_by_time = spark.sql("""
    select 
        from_unixtime(unix_timestamp(timestamp,"yyyy/MM/dd hh:mm:ss"),'yyyy/MM/dd hh') time_bucket,
        concat(split(url_index1,"[.]")[1], ".", split(url_index1,"[.]")[2]) domain,
        url_index2,
        count(event_id) event_counts,
        count(distinct user.id) user_counts
    from event_master
    group by time_bucket,domain,url_index2
    order by domain,time_bucket,url_index2 """)
activities_by_time.show(5)


trade_demand_transform = spark.sql("""
    with trade as(
        select 
            user.id user_id,
            url_index3 trade,
            from_unixtime(unix_timestamp(timestamp,"yyyy/MM/dd hh:mm:ss"),'yyyyMM') month_bucket
        from event_master
        where locate("/find/", cleaned_url) > 0
    ), frequency as(
    select
        trade,
        month_bucket,
        count(user_id) over(partition by month_bucket,trade) search_frequency
     from trade
    ), ranked as(
    select
        *,
        row_number() over(partition by month_bucket order by search_frequency desc) rnk
    from frequency
    )
    select
        month_bucket,
        trade,
        search_frequency
    from ranked where rnk = 1 order by month_bucket
""")
trade_demand_transform.show()