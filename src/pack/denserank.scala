package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object denserank {

def main(args:Array[String]):Unit={

			println("===Hello====")

			val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.host","localhost")
			.set("spark.driver.allowMultipleContexts", "true")

			val sc = new SparkContext(conf)

			sc.setLogLevel("ERROR")

			val spark = SparkSession.builder.getOrCreate()

			import spark.implicits._

		val df = Seq(("DEPT1", 1000), 
		    ("DEPT1", 500), 
		    ("DEPT1", 700), 
		    ("DEPT2", 400), 
		    ("DEPT2", 200), 
		    ("DEPT2", 400), 
		    ("DEPT2", 200),
		    ("DEPT3", 500), 
		    ("DEPT3", 200))
         .toDF("department", "salary")

         df.show()
         
         
  val windowdata=Window.partitionBy("department")
                 .orderBy(col("salary")desc)

                 
   val rankdf=df.withColumn("denserank",dense_rank()over windowdata)

    rankdf.show()
			

val finaldf=rankdf.filter(col("denserank")===2)
                   .drop("densrank")

	}

}