import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.recommendation.{ALS,MatrixFactorizationModel,Rating}


object movies{

def main(args:Array[String] ){



//initialize
val sc = new SparkContext()
val sqlContext = new SQLContext(sc)

//data setup
val ratingsDF = sqlContext.read.format("com.databricks.spark.csv").option("inferschema","true").option("header","true").load("input/ratings.csv")
val moviesDF = sqlContext.read.format("com.databricks.spark.csv").option("inferschema","true").option("header","true").load("input/movies.csv")
ratingsDF.registerTempTable("ratings")
moviesDF.registerTempTable("movies")
val ratingsRDD = ratingsDF.rdd
val moviesRDD = moviesDF.rdd

//analysis
val x= sqlContext.sql("select title,avg(rating) as rating,count(0) as cnt from movies,ratings where movies.movieId = ratings.movieId group by title order by rating desc,cnt desc")

//x.write.save("output/movies.parquet")
//x.show(10)

//machine learning movie recommendation
val rating = ratingsRDD.map{ x => Rating(x.getInt(0),x.getInt(1),x.getDouble(2))}
val ranking = 10
val numIterations=10
val model = ALS.train(rating, ranking, numIterations)
val userPreferences=model.recommendProducts(args(0).toInt,5)
val schema = StructType(Array(StructField("user_Id",IntegerType,false),StructField("movie_id",IntegerType,false),StructField("rating",DoubleType,false)))
val userRDD = sc.parallelize(userPreferences.map{case Rating(user,product,rating) => Row(user,product,rating)})
val userDF=sqlContext.createDataFrame(userRDD,schema)
userDF.registerTempTable("userPreferences")
sqlContext.sql("select title,genres,rating from userPreferences,movies where movies.movieId=userPreferences.movie_id order by rating desc").show()

println("Arguement passed"+args(0))
sc.stop()

}

}

