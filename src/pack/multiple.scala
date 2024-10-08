package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object multiple {

def main(args:Array[String]):Unit={

			println("===Hello====")

			val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.host","localhost")
			.set("spark.driver.allowMultipleContexts", "true")

			val sc = new SparkContext(conf)

			sc.setLogLevel("ERROR")

			val spark = SparkSession.builder.getOrCreate()

			import spark.implicits._
		
val data = Seq(
  ("Alice", "Sales", 5000),
  ("Akshay", "Sales", 5000),
  ("Bob", "Sales", 4500),
  ("Charlie", "Sales", 4000),
  ("Akash", "Sales", 5500),
  ("David", "HR", 6000),
  ("Eva", "HR", 5500),
 ("Tom", "HR", 5500),
 ("crlos", "HR", 5500),
  ("cat", "HR", 5000)
).toDF("name", "department", "salary")

// Define a window specification
val windowSpec = Window.partitionBy("department").orderBy(col("salary").desc)

// Add ranking columns
val rankedDF = data
  .withColumn("row_number", row_number().over(windowSpec))
  .withColumn("rank", rank().over(windowSpec))
  .withColumn("dense_rank", dense_rank().over(windowSpec))

rankedDF.show()




val lagLeadDF = data
  .withColumn("lag", lag("salary", 1).over(windowSpec))
  .withColumn("lead", lead("salary", 1).over(windowSpec))

lagLeadDF.show()



// Use SUM() to calculate a cumulative sum of salaries within a department
val aggDF = data
  .withColumn("total_salary", sum("salary").over(windowSpec))

aggDF.show()


}

}