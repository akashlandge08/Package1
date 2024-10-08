package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object agg {

def main(args:Array[String]):Unit={

			println("===Hello====")

			val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.host","localhost")
			.set("spark.driver.allowMultipleContexts", "true")

			val sc = new SparkContext(conf)

			sc.setLogLevel("ERROR")

			val spark = SparkSession.builder.getOrCreate()

			import spark.implicits._

			val df = Seq(
					("sai", 10),
					("zeyo",5),
					("sai", 20),
					("zeyo",10),
					("sai", 5)
					).toDF("name","amount")

			df.show()
			
			
			val aggdf= df.groupBy("name")
			              .agg(sum("amount").cast(IntegerType).as("total"),
			               count("amount").cast(IntegerType).as("count")
			               )
			               
			               aggdf.show()
			                  



			



	}

}