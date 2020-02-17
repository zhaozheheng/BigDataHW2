from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


conf = SparkConf().setMaster("local").setAppName("q4")
sc = SparkContext(conf=conf)

spark = SparkSession(sc)
# data is in the same folder of this script
# load data
business = sc.textFile("business.csv").map(lambda line: line.split("::")).toDF(["bus_id", "address", "categories"]).distinct()
business_filtered = business.filter(business.address.contains("NY"))
review = sc.textFile("review.csv").map(lambda line: line.split("::")).toDF(["review_id", "user_id", "bus_id", "stars"]).distinct()

total_table = business_filtered.join(review, "bus_id")

avg_rating_table = total_table.groupBy("bus_id").agg(F.mean("stars").alias("avg_rating")).join(business_filtered, "bus_id")

# get desired result and sort in ascending order with top 20 items (actually tail 20)
res = avg_rating_table.select("bus_id", "address", "categories", "avg_rating").orderBy(["avg_rating"], ascending=[1]).limit(20)

# res.show()
# save file, no column name, no index number, tab sequence
res.toPandas().to_csv("q4_result.csv", header=0, index=0, sep='\t')