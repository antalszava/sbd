//Lab1 Supercomputing for BigData
//Group 17
//Antal Száva, Student ID: 4958489
//Maryam Tavakkoli, Student ID: 4956222

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.sql.Date

import scala.collection.mutable

object lab1 {
  case class GlobalData (
    GKGRECORDID: String,
	DATE:Date,
	SourceCollectionIdentifier: Integer,
	SourceCommonName: String,
	DocumentIdentifier: String,
	Counts: String,
	V2Counts: String,
	Themes: String,
	V2Themes: String,
	Locations: String,
	V2Locations: String,
	Persons: String,
	V2Persons: String,
	Organizations: String,
	V2Organizations: String,
	V2Tone: String,
	Dates: String,
	GCAM: String,
	SharingImage: String,
	RelatedImages: String,
	SocialImageEmbeds: String,
	SocialVideoEmbeds: String,
	Quotations: String,
	AllNames: String,
	Amounts: String,
	TranslationInfo: String,
	Extras: String
  )

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val schema =
	  StructType(
 	    Array(
 	      StructField("GKGRECORDID", StringType, nullable=true),
		  StructField("DATE", DateType, nullable=true),
		  StructField("SourceCollectionIdentifier", IntegerType, nullable=true),
		  StructField("SourceCommonName", StringType, nullable=true),
		  StructField("DocumentIdentifier", StringType, nullable=true),
		  StructField("Counts", StringType, nullable=true),
		  StructField("V2Counts", StringType, nullable=true),
		  StructField("Themes", StringType, nullable=true),
		  StructField("V2Themes", StringType, nullable=true),
		  StructField("Locations", StringType, nullable=true),
		  StructField("V2Locations", StringType, nullable=true),
		  StructField("Persons", StringType, nullable=true),
		  StructField("V2Persons", StringType, nullable=true),
		  StructField("Organizations", StringType, nullable=true),
		  StructField("V2Organizations", StringType, nullable=true),
		  StructField("V2Tone", StringType, nullable=true),
		  StructField("Dates", StringType, nullable=true),
		  StructField("GCAM", StringType, nullable=true),
		  StructField("SharingImage", StringType, nullable=true),
		  StructField("RelatedImages", StringType, nullable=true),
		  StructField("SocialImageEmbeds", StringType, nullable=true),
		  StructField("SocialVideoEmbeds", StringType, nullable=true),
		  StructField("Quotations", StringType, nullable=true),
		  StructField("AllNames", StringType, nullable=true),
		  StructField("Amounts", StringType, nullable=true),
		  StructField("TranslationInfo", StringType, nullable=true),
		  StructField("Extras", StringType, nullable=true)
        )
      )
    val startTime = System.currentTimeMillis
	val spark = SparkSession
      .builder
      .appName("Lab1")
      .config("spark.master", "local")
      .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._

    val df = spark.read
              .schema(schema)
              .option("dateFormat", "YYYYMMDDHHMMSS")
              .option("delimiter", "\t")  //???????????
              .csv("./segment10/*.csv")
              .as[GlobalData]


    val removeoffset = udf((data : String) => data.substring(0, data.lastIndexOf(',')))
    val merge = udf((col1 : String, col2 : String) => (col1, col2))
    val tenresult = udf((wrappedArray : mutable.WrappedArray[(String,String)]) => wrappedArray.take(10))

    val ds = df.select($"DATE", $"AllNames")
    //Commands for transformation to get the right data
    val filterRdd = ds
      .withColumn("AllNames", split(ds("AllNames"), ";"))
      .withColumn("AllNames", explode($"AllNames"))
      .filter("AllNames != 'null'")
      .filter("AllNames != 'Type ParentCategory'")
      .withColumn("AllNames", removeoffset($"AllNames"))
      .groupBy("DATE", "AllNames").count()
      .orderBy(desc("count"))
      .withColumn("AllNames", merge($"AllNames", $"count"))
      .groupBy("DATE").agg(collect_list("AllNames").as("AllNames")) //group by date
      .withColumn("AllNames", tenresult($"AllNames"))
      .collect.foreach(println)

    val endTime = System.currentTimeMillis
    println("Total Run Time: " + (endTime - startTime) / 1000 + " seconds.")
    spark.stop
  }
}
//val verynds = newds.withColumn("AllNames", split(newds("AllNames"), ",").cast("array<String>")(0))
//val uuu = newds.union(new2)