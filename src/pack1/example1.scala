package pack1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object example1 {


 def main(args:Array[String]):Unit={

   println("===Hello====")

   val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.host","localhost")
   .set("spark.driver.allowMultipleContexts", "true")

   val sc = new SparkContext(conf)

   sc.setLogLevel("ERROR")

   val spark = SparkSession.builder.getOrCreate()

   import spark.implicits._



   val df = Seq(
     (1, 80000, 2020),
     (1, 70000, 2019),
     (1, 60000, 2018),
     (2, 65000, 2020),
     (2, 65000, 2019),
     (2, 60000, 2018),
     (3, 65000, 2019),
     (3, 60000, 2018)
     ).toDF("employee_id", "salary", "year").orderBy()

   df.show()
   

   val windowspec =   Window.partitionBy("employee_id").orderBy("year")
   
   
   val resultdf = df.withColumn("last_year_salary",lag("salary",1).over(windowspec))
   
   
   resultdf.show()
   
   
   
   val salarydiff = resultdf.withColumn("salary_diff",expr("salary-last_year_salary"))
                   .select("employee_id","year","salary","salary_diff")
   
   salarydiff.show()
   
   

 }
 }

