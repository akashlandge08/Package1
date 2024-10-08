package pack1
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._



import org.apache.spark.sql.SparkSession


object rddtodf {
  
  
    def main (args:Array[String]):Unit={
      
      println("===========started===========")
      
      
      val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.host","localhost")
.set("spark.driver.allowMultipleContexts", "true")

val sc = new SparkContext(conf)

sc.setLogLevel("ERROR")

val spark = SparkSession.builder.getOrCreate()

import spark.implicits._

case class Person(name: String, age: Int)


val rdd = (Seq(("Alice", 25), ("Bob", 30)))

println(rdd)

val df = rdd.toDF("name", "age")
df.show()


val ds = Seq(("Alice", 25), ("Bob", 30)).toDS()

val df1 = ds.toDF()
df1.show()



}
}