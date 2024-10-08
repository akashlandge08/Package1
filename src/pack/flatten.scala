package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object flatten {

def main(args:Array[String]):Unit={

			println("===Hello====")

			val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.host","localhost")
			.set("spark.driver.allowMultipleContexts", "true")

			val sc = new SparkContext(conf)

			sc.setLogLevel("ERROR")

			val spark = SparkSession.builder.getOrCreate()

			import spark.implicits._
			
			val data = Seq(
  (1, "Alice", Array("Math", "Science")),
  (2, "Bob", Array("English", "History")),
  (3, "Charlie", Array("Math", "Geography"))
).toDF("id", "name", "subjects")

data.show()

val flattenedDF = data.withColumn("subject", explode(col("subjects"))).drop("subjects")

flattenedDF.show()


			
			
			
			
}

}
			