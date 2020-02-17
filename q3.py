from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession

conf = SparkConf().setMaster("local").setAppName("q3")
sc = SparkContext(conf=conf)

spark = SparkSession(sc)

business = sc.textFile("business.csv").map(lambda line: line.split("::")).toDF(["bus_id", "address", "categories"]).distinct()
review = sc.textFile("review.csv").map(lambda line: line.split("::")).toDF(["review_id", "user_id", "bus_id", "stars"]).distinct()
user = sc.textFile("user.csv").map(lambda line: line.split("::")).toDF(["user_id", "name", "url"]).distinct()

business_filtered = business.filter(business.categories.contains("Colleges & Universities"))
college_review = business_filtered.join(review, "bus_id")
college_review_users = college_review.join(user, "user_id")

res = college_review_users.select("user_id", "name", "stars")

# res.show()
# no column name, no index number, tab sequence
res.toPandas().to_csv("q3_result.csv", header=0, index=0, sep='\t')