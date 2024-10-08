package pack
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._

object filer {
  
  def main(args:Array[String]):Unit={
    val spark = SparkSession.builder()
      .appName("CSV to StringType")
      .master("local[*]") 
      .getOrCreate()

   
 import spark.implicits._
    val df = spark.read
                        .format("csv")
                      .option("header", "true")
                     .load("file:///E:/file3.txt")

   
    val stringDF = convertAllColumnsToString(df)

    
    stringDF.show()

  
  }

  def convertAllColumnsToString(df: DataFrame): DataFrame = {
    
    val stringColumns = df.columns.map(c => df.col(c).cast(StringType))
    df.select(stringColumns: _*)
  }
}
