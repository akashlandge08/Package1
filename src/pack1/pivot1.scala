package pack1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object pivot {




 def main(args:Array[String]):Unit={

   println("===Hello====")

   val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.host","localhost")
   .set("spark.driver.allowMultipleContexts", "true")

   val sc = new SparkContext(conf)

   sc.setLogLevel("ERROR")

   val spark = SparkSession.builder.getOrCreate()

   import spark.implicits._
   
 val df=Seq(
       (1,"id","1001"),
       (1,"Name","adi"),
       (2,"id","1002"),
       (2,"Name","vas"))
       .toDF("pid","keys","values")
       
       df.show()
       
   val pivotdf=df.groupBy("pid")
   .pivot("keys")
   .agg(first("values"))
   
   pivotdf.show()
   
   
   
   
   
   
   
   
   
   
 }
 }
