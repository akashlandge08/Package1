package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object spark {
  
  


def main(args:Array[String]):Unit={

		println("===Hello====")

		val conf = new SparkConf().setAppName("first").setMaster("local[*]").set("spark.driver.host","localhost")
		.set("spark.driver.allowMultipleContexts", "true")

		val sc = new SparkContext(conf)

		sc.setLogLevel("ERROR")

		val spark = SparkSession.builder.getOrCreate()

		import spark.implicits._
		
		val  file1 =  sc.textFile("file:///E:/abc.txt")

		println
		println("=====raw file1===")
		println

		file1.take(5).foreach(println)
		

val gymdata =file1.filter( x => x.contains("Gymnastics") )


		println
		println("=====Gym Data===")
		println  
		gymdata.foreach(println)
		
		val mapsplit = gymdata.map( x => x.split(","))
		
		
		
val csvdf= spark.read
                .format("csv")
                .option("header","true")
                .load("file:///E:/file3.txt")
             println
		println("=====csvdf===")
		println		

		csvdf.show(5)  
		 
		
val json= spark.read.format("json").load("file:///E:/file4.json")
		println
		println("=====jsondf===")
		println		
json.show(5)
		
val parquetdf=spark.read.load("file:///E:/file5.parquet")
println
println("parquetdf")
		println
		parquetdf.show(5)
		
		
			val xmldf = spark
			             .read.format("xml").option("rowtag","txndata")
		                .load("file:///E:/file6")
		                
		                println
		println("===== xmldf===")
		println	
		xmldf.show(5)
		
		
val collist= List("txnno","txndate","custno", "amount" , "category" ,"product" ,"city" , "state","spendby")		
		
val newjsondf=json.select(collist.map(col):_*)

println
		println("===== new jsondf===")
		println	
		newjsondf.show(5)
		

		val newxmldf=json.select(collist.map(col):_*)
		println
		println("===== new xmldf===")
		println	
		newxmldf.show(5)
	
val  uniondf =  csvdf.union(newjsondf).union(parquetdf).union(newxmldf)


		println
		println("=====uniondf===")
		println		

		uniondf.show(5)				

val dsldf=uniondf
           .withColumn("txndate",expr("split(txndate,'-')[2]"))
           .withColumnRenamed("txndate","year")
           .withColumn("status",expr("case when spendby='cash'then 1 else 0 end"))
           .filter(col("txnno")>50000)
		
	println
		println("=====dsldf===")
		println		

		dsldf.show(5)		
		
val aggdf=  	dsldf.groupBy("category")	
                   .agg(sum("amount").cast(IntegerType).as("total"))
                       
                       
    println
		println("=====aggdf===")
		println		
		aggdf.show()
		
}
}
		
		
		
		
		
		
		
		