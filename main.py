#Lydia Athanasiou F3312102
#Sofia Drougka F3312105

from doctest import master
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyspark
from pyspark.shell import spark
from pyspark import SparkContext, SparkConf

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.types import StructType, StringType, IntegerType, StructField

sc = SparkContext.getOrCreate()


# sc = pyspark.SparkContext('local[*]')

def export_groupby(filename, value):
    f = open(filename, 'w')
    f.write(value)
    f.close


def print_hi(name):
    print("================Question 1====================")

    usersdf = spark.read.option("delimiter", "|").option("header", "true").option("inferSchema", "true").csv(
        "USERS.txt")
    usersdf.show()

    usermoviesdf = spark.read.option("delimiter", "|").option("header", "true").option("inferSchema", "true").csv(
        "USER_MOVIES.txt")
    usermoviesdf.show()

    moviesgenredf = spark.read.option("delimiter", "|").option("header", "true").option("inferSchema", "true").csv(
        "MOVIES_GENRE.txt")
    moviesgenredf.show()

    usermv_mvgenre = usermoviesdf.join(moviesgenredf, usermoviesdf.mid == moviesgenredf.mid, "inner").select("userid",
                                                                                                             "genre",
                                                                                                             "rating")
    usermv_mvgenre.show()

    usr_usrmvs = usersdf.join(usermv_mvgenre, usersdf.userid == usermv_mvgenre.userid, "inner").select("genre",
                                                                                                       "gender",
                                                                                                       "rating")
    usr_usrmvs.show()
    # usr_usrmvs.cube("genre", "gender")
    # df = usr_usrmvs.toDF("gender", "genre", "rating").cube("genre", "gender").avg("rating")
    # df.show()

    df = usr_usrmvs.cube("genre", "gender").avg("rating").withColumnRenamed("avg(rating)", "avgRating").sort("genre",
                                                                                                             "gender")
    df.show()
    # Save group by 1 genre-gender
    f = open("genre_gender.txt", 'a')
    datacube = df
    datacube.toPandas().to_csv(f)

    # Save group by 2 genre-null
    f = open("genre_null.txt", 'a')
    datacube2 = df
    datacube2 = datacube2.filter("gender IS NULL").toPandas()
    datacube2.to_csv("genre_null.txt")

    # save group by 3 null-gender
    f = open("null_gender.txt", 'a')
    datacube3 = df
    datacube3 = datacube3.filter("genre IS NULL").toPandas()
    datacube3.to_csv("null_gender.txt")

    # save group by 4 null-null
    f = open("null_null.txt", 'a')
    datacube4 = df
    datacube4 = datacube4.filter("genre IS NULL").filter("gender IS NULL").toPandas()
    datacube4.to_csv("null_null.txt")

    print("================Question 2====================")
    femAvg = df.select("genre", "gender", "avgRating").withColumnRenamed("avgRating", "FavgRating").filter("gender == "
                                                                                                           "'F'")
    femAvg.filter("genre IS NOT NULL").show()

    malAvg = df.select("genre", "gender", "avgRating").withColumnRenamed("avgRating", "MavgRating").filter("gender == "
                                                                                                           "'M'")
    malAvg.filter("genre IS NOT NULL").show()

    femAvg.join(malAvg, femAvg.genre == malAvg.genre, "inner").drop("femAvg.genre").filter(
        femAvg.FavgRating > malAvg.MavgRating).show()

    # cubeTable = df.createOrReplaceTempView("cubeTable")
    # f = open("atr1.txt", 'a')
    # df.foreach(lambda x:

    # print("Data ==>" + x["genre"] + "," + x["gender"] + "," + x["avg(rating)"])
    # f.writelines(x["genre"] + "," + x["gender"] + "," + x["avg(rating)"])
    # )
    # group1 = df.groupBy("genre", "gender")
    # group2 = df.filter("gender = 'F'").show()
    # for r in df:
    # export_groupby(r.getField(0) + "_" + r.getField(1), r.getField(2))
    # print(r.getField(0))

    # np.savetxt(r'Attr1_Attr2.txt', df., fmt='%d')
    ################################################################

    # df1 = usr_usrmvs.where("gender=F") df1 = usr_usrmvs.cube("genre", "gender").avg("rating").sort("genre",
    # "gender").filter(usr_usrmvs.filter("F")).show()
    # df.filter("gender == 'F'").withColumnRenamed("avg(rating)", "femaleRating").show()
    # df.filter("gender == 'M'").withColumnRenamed("avg(rating)", "maleRating").show()
    # df.select("femaleRating").astype(float)
    # df.select("maleRating").astype(float)
    # output = df.filter("gender == 'F'").join(df.filter("gender == 'M' "),
    # df.filter("gender == 'M' ").genre == df.filter("gender == 'F'").genre)

    # output.filter("gender == 'F'").show()
    # output.filter("gender == 'M'").show()

    # output.show()
    # out = output.groupBy("genre").count().show()
    # spark.sql("SELECT * FROM cubeTable").show()

    # fratings = spark.sql(""" SELECT gender, genre, "avg(rating)" FROM cubeTable WHERE gender="F" """).toDF()
    # mratings = spark.sql("""SELECT gender , genre , "avg(rating)" FROM cubeTable WHERE gender="M"
    # """).toDF()
    # fratings.withColumnRenamed("avg(rating)", "rating").show()
    # mratings.withColumnRenamed("avg(rating)", "rating").show()

    # df1 = fratings.select("gender", "genre", "avg(rating)").where("avg(rating)" > (mratings.select("avg(
    # rating)").filter("mratings.genre == fratings.genre")))

    # result = fratings.join(mratings, fratings.genre == mratings.genre, "inner").select("genre", "gender", "avg(rating)")

    # result.show()

    # spark.sql("""SELECT  FROM cubeTable where genre="Drama" """).show()
    # spark.sql("""SELECT gender, genre "avg(rating)" FROM cubeTable as c1 where gender="F" and "avg(rating)" > (SELECT
    # MAX("avg( rating)") FROM cubeTable as c2 where gender="M") """).show()

    # output.filter("gender == 'F'").withColumnRenamed("avg(rating)", "femaleRating").show()
    # output.filter("gender == 'M'").withColumnRenamed("avg(rating)", "maleRating").show()
    # output.select("genre").filter("femaleRating" > "maleRating").show()

    # df.toPandas().to_csv("atr1.txt")

    print("==========================EROTHMA 3============================")
    # quest3 = usermv_mvgenre.createOrReplaceTempView("ratingsTable")
    # q3 = spark.sql(
    # """select rating, count(rating) as cnt from ratingsTable group by genre where genre = "Comedy" """).toDF().show()
    # x = q3.rating
    # y = q3.cnt
    # plt.hist(x, y)
    # plt.show()
    print("=============NUMBER OF REVIEWS PER RATING FOR COMEDY============")
    graph = usermv_mvgenre.select("genre", "rating").filter("genre= 'Comedy'").groupBy("rating").count().sort(
        "rating").show()
    graph1 = usermv_mvgenre.select("genre", "rating").filter("genre= 'Comedy'").groupBy("rating").count().sort(
        "rating").toPandas()
    pltlist = graph1["count"]
    xaxis = graph1["rating"]

    print("=============TOTAL NUMBER OF REVIEWS PER RATING ================")
    usermv_mvgenre.groupBy("genre", "rating").count().sort("rating").show()
    # plt.bar(range(5), [7490, 16845, 39466, 52748, 36495])

    barplot = plt.bar(xaxis, pltlist, color=['black', 'red', 'green', 'blue', 'cyan'])
    plt.title('Graph for comedy')
    plt.xlabel("ratings")
    plt.ylabel("Number of evaluations")
    plt.show()

    # plotList = usermv_mvgenre.select("genre", "rating").filter("genre= 'Comedy'").groupBy(
    # "rating").count().toPandas().iterrows()
    # graph2 = graph1.toPandas()
    # plotList = graph2.iterrows()
    # plotList = list(plotList)
    # plotLIST1 = []
    # for row in plotList:
    #   plotLIST1.append(row[1])
    # print(plotLIST1)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
