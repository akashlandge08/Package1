package pack1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object example2 {




 def main(args:Array[String]):Unit={

   println("===Hello====")

   val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.host","localhost")
   .set("spark.driver.allowMultipleContexts", "true")

   val sc = new SparkContext(conf)

   sc.setLogLevel("ERROR")

   val spark = SparkSession.builder.getOrCreate()

   import spark.implicits._


   val df1 = Seq(
     "1", "2","3"
     ).toDF("col")    


   df1.show()  



   val df2 = Seq(
     "1", "2","3","4","5"
     ).toDF("col")    


   df2.show()  


   val maxdf = df1.selectExpr("max(col) as col")
   
   
   maxdf.show()
   
   
   
  val antijoin = df2.join(maxdf,Seq("col"),"left_anti")
  
  antijoin.show()
   
   
   


 }

}