import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object movies{

def main(args:Array[String] ){
//initialize
val sc = new SparkContext()
val sqlContext = new SQLContext(sc)

//data setup
val ratingsDF = sqlContext.read.format("com.databricks.spark.csv").option("inferschema","true").option("header","true").load("C:\\cs518_hadoop\\moviesmall\\ml-latest-small\\ratings.csv").registerTempTable("ratings")
val moviesDF = sqlContext.read.format("com.databricks.spark.csv").option("inferschema","true").option("header","true").load("C:\\cs518_hadoop\\moviesmall\\ml-latest-small\\movies.csv").registerTempTable("movies")


val x= sqlContext.sql("select * from movies")

x.take(10).foreach(println)

}

}

