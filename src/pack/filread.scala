package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object filread {

def main(args:Array[String]):Unit={

			println("===Hello====")

			val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.host","localhost")
			.set("spark.driver.allowMultipleContexts", "true")

			val sc = new SparkContext(conf)

			sc.setLogLevel("ERROR")

			val spark = SparkSession.builder.getOrCreate()

			import spark.implicits._
			
		

			val avrodf   =  spark
			                .read
			                .format("avro")
			                .load("file:///E:/part.avro")   // GIVE YOUR PATH
			
			
			avrodf.show()
			

     
			val xmldf =    spark
			               .read
			               .format("xml")
			               .option("rowtag","book")
			               .load("file:///E:/book.xml")   // GIVE YOUR PATH
			
			
			xmldf.show()
			
			
			val jsonfile= spark
			               .read
			               .format("json")
			              .load("file:///E:/devices.json")  
    
jsonfile.show()
	}

}