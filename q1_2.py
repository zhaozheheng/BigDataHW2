from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import udf

def mutual_cal(lis1, lis2):
    intersect = []
    for item in lis1:
        if item in lis2:
            intersect.append(item)
    return intersect

def friend_map(line):
    vals = line.split('\t')
    keys = []
    user = vals[0]
    fri_list = vals[1].split(',')
    for friend in fri_list:
        if int(user) < int(friend):
            pair = (user, friend)
        else:
            pair = (friend, user)
        keys.append((pair, fri_list))
    return keys

def friend_reduce(val1, val2):
    # print(val1)
    # print(val2)
    # print(mutual_cal(val1, val2))
    # print("--------------------------------------------")
    # if len(mutual_cal(val1, val2)) != 0:
    #     return mutual_cal(val1, val2)
    # else:
    #     return None
    return len(mutual_cal(val1, val2))
    # return 1

# question 1
today = date.today
thisyear = today().year

conf = SparkConf().setMaster("local").setAppName("q1")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

# question1 part
# data = sc.textFile("testdata.txt")
data = sc.textFile("soc-LiveJournal1Adj.txt")
data = data.filter(lambda line: line.split('\t')[-1] is not '').flatMap(friend_map)

friend_details = data.reduceByKey(friend_reduce)
friend_details = friend_details.map(lambda x: (x[0][0], x[0][1], x[1]))
# table = data.toDF()
# table.show()
# tab3.show()
# print(friend_details.count())
# print(tab3.count())
mutual_friends = friend_details.toDF(["user1", "user2", "nums"])
# print(table.count())
mutual_friends.show()
# save file, no column name, no index number, tab sequence
mutual_friends.toPandas().to_csv("q1_result.csv", header=0, index=0, sep='\t')

# question2 part
# start question2
sorted_res = mutual_friends.orderBy(["nums"], ascending=[0]).limit(10)

# load userdata file and setup last column as their age
user_details = sc.textFile("userdata.txt").map(lambda x: x.split(',')).map(lambda x: [int(x[i]) if i == 0
else thisyear - int(x[i].split('/')[-1]) if i == len(x) - 1 else x[i] for i in range(len(x))])

user_details_table = user_details.toDF(["user_id", "fname", "lname", "address", "city", "state", "zipcode", "country", "username", "age"])
refine_user_details = user_details_table.select("user_id", "fname", "city", "age")

# start get wnd result
step1 = sorted_res.join(refine_user_details, sorted_res.user1 == refine_user_details.user_id).select(sorted_res.nums,
sorted_res.user1.alias("user1"), sorted_res.user2.alias("user2"), refine_user_details.fname.alias("user1_fname"),
refine_user_details.city.alias("user1_city"), refine_user_details.age.alias("user1_age"))

step2 = step1.join(refine_user_details, step1.user2 == refine_user_details.user_id).select(step1.nums, step1.user1_fname,
step1.user1_city, step1.user1_age, refine_user_details.fname.alias("user2_fname"), refine_user_details.city.alias("user2_city"),
refine_user_details.age.alias("user2_age"))

step2.show()
# save file, no column name, no index number, tab sequence
step2.toPandas().to_csv("q2_result.csv", header=0, index=0, sep='\t')