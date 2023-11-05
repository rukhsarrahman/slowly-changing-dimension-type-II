from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_date, when, col, max, row_number
from datetime import date
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("SCD Type II Example").getOrCreate()

current_df = spark.read.options(header='True', inferSchema='True', delimiter=',').csv("employee_data.csv")
current_df.show()
today = str(date.today())
eot = "9999-12-12"
new_data = [(5,"Johan","Liebert",35,"liebejohan@google.com","Germany",today,eot),
            (2,"Emmalyn","Orritt",46,"eorritt1@liveinternet.ru","Italy",today,eot)
            ]
new_df = spark.createDataFrame(new_data, current_df.columns)
primary_key = ["employee_id"]
updated_records = new_df.join(current_df, on=primary_key,how="leftsemi")
inserted_records = new_df.join(current_df, on=primary_key, how="left_anti")

current_df = current_df.withColumn(
    "end_date",
    when(current_df["employee_id"].isin(updated_records.select("employee_id").rdd.flatMap(list).collect()), lit(current_date())).otherwise(current_df["end_date"])
)

max_id = current_df.select(max("employee_id")).first()[0]
inserted_records = inserted_records.withColumn("employee_id", max_id + row_number().over(Window.orderBy(lit(1))))
current_df = current_df.union(inserted_records)
max_id = current_df.select(max("employee_id")).first()[0]
updated_records = updated_records.withColumn("employee_id", max_id + row_number().over(Window.orderBy(lit(1))))
current_df = current_df.union(updated_records)

current_df.show()
rdd = current_df.rdd.map(lambda row: ",".join(map(str, row)))
rdd = rdd.coalesce(1)
rdd.saveAsTextFile("output")

spark.stop()
