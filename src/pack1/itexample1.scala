package pack1


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object itexample1 {
  

  def main(args:Array[String]):Unit={

val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.host","localhost")
.set("spark.driver.allowMultipleContexts", "true")

val sc = new SparkContext(conf)

sc.setLogLevel("ERROR")


val spark = SparkSession.builder.appName("Expand Rows").getOrCreate()
import spark.implicits._

// Input data
val data = Seq(
  (1, "Arun,Komal"),
  (2, "Prabal,Veenet"),
  (3, "Sanjana")
)

// Creating DataFrame
val df = data.toDF("ID", "Name")


 df.show()


// Split the "Name" column by commas and explode it into multiple rows
val resultDF = df.withColumn("Name", explode(split(col("Name"), ",")))


resultDF.show()


}
}
