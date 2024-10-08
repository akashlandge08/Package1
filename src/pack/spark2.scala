package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

object spark2 {



case class schema(

		txnno:String,
		txndate:String,
		custno:String,
		amount:String,
		category:String,
		product:String,
		city:String,
		state:String,
		spendby:String

		)



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




		val gymdata = file1.filter( x => x.contains("Gymnastics") )


		println
		println("=====gymdata===")
		println

		gymdata.take(5).foreach(println)			


		val mapsplit = gymdata.map( x => x.split(","))

		val schemardd = mapsplit.map(x => schema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))

		val prodfilter = schemardd.filter( x => x.product.contains("Gymnastics"))

		println
		println("=====prodfilter===")
		println

		prodfilter.take(5).foreach(println)						



		val schemadf =   prodfilter.toDF()  

		println
		println("=====schemadf===")
		println		

		schemadf.show(5)



		val csvdf = spark.read.format("csv").option("header","true").load("file:///D:/file3.txt")

		println
		println("=====csvdf===")
		println		

		csvdf.show(5)		


		val jsondf = spark.read.format("json").load("file:///D:/file4.json")


		println
		println("=====jsondf===")
		println		

		jsondf.show(5)		


		val parquetdf = spark.read.load("file:///D:/file5.parquet")

		println
		println("=====parquetdf===")
		println		

		parquetdf.show(5)			


		val xmldf = spark.read.format("xml").option("rowtag","txndata")
		.load("file:///D:/file6")


		println
		println("=====xmldf===")
		println		

		xmldf.show(5)	




		val collist = List("txnno","txndate","custno", "amount" , "category" ,"product" ,"city" , "state","spendby")



		val  newjsondf = jsondf.select(collist.map(col): _*)


		println
		println("=====newjsondf===")
		println		

		newjsondf.show(5)		



		val newxmldf = xmldf.select(collist.map(col): _*)

		println
		println("=====newxmldf===")
		println		

		newxmldf.show(5)				




		val  uniondf =  schemadf.union(csvdf).union(newjsondf).union(parquetdf).union(newxmldf)


		println
		println("=====uniondf===")
		println		

		uniondf.show(5)				




		val dsldf  = uniondf
		.withColumn("txndate",expr("split(txndate,'-')[2]"))
		.withColumnRenamed("txndate","year")
		.withColumn("status",expr("case when spendby='cash' then 1 else 0 end"))
		.filter(col("txnno") > 50000 )




		println
		println("=====dsldf===")
		println		

		dsldf.show(5)		





		val aggdf = dsldf.groupBy("category")
		.agg(sum("amount").cast(IntegerType).as("total"))

		println
		println("=====aggdf===")
		println		

		aggdf.show()				

		//  aggdf.coalesce(1).write.format("csv").mode("overwrite").save("file:///D:/revwrite")



		val df = Seq(
				(1, "raj"),
				(2, "ravi"),
				(3, "sai"),
				(5, "rani")
				).toDF("id","name")

		df.show()



		val df1 = Seq(
				(1, "mouse"),
				(3, "mobile"),
				(7, "laptop")
				).toDF("id","product")    


		df1.show()	



		val inner = df.join(df1,Seq("id"),"inner")


		println
		println("=====inner===")
		println		

		inner.show()		


		val left = df.join(df1,Seq("id"),"left")


		println
		println("=====left===")
		println		

		left.show()		



		val right = df.join(df1,Seq("id"),"right")


		println
		println("=====right===")
		println		

		right.show()		


		val full = df.join(df1,Seq("id"),"full")


		println
		println("=====full===")
		println		

		full.show()				




		val cross = df.crossJoin(df1)

		println
		println("=====cross===")
		println		

		cross.show()				


		val leftanti = df.join(df1,Seq("id"),"left_anti")


		println
		println("=====leftanti===")
		println		

		leftanti.show()				







}

}