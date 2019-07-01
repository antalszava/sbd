//Lab1 Supercomputing for BigData
//Group 17
//Antal Száva, Student ID: 4958489
//Maryam Tavakkoli, Student ID: 4956222
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import java.sql.Timestamp
import org.apache.spark.rdd.RDD

object Lab1 {
  def main(args: Array[String]) {
    val startTime = System.currentTimeMillis
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark = SparkSession
      .builder
      .appName("Lab1")
      .config("spark.master", "local")
      .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._

    val file = sc.textFile("./segment10/*.csv")
    val mapRdd = file.map(_.split("\t"))
                     .filter(x => x != null)
                     // .filter(x => x != "Type ParentCategory")//Remove False positive
                     .filter(_.length > 23)
                     .map(x => (x(1).take(8), x(23).split(";"))) // (Date, (Topic, Count))

    val filterRdd = mapRdd.map {case (x, y) => ( x, y.map(x => x.split(",")(0)))} // (Date, Array(Topic Name))
    val flatRdd = filterRdd.flatMapValues(x => x) // (Data, Topic Name)
    val reduceRdd = (flatRdd.map { case (x, y) => ((x, y), 1) }).reduceByKey((x, y) => x + y) // ((Date, Topic Name), Count)
    val countRdd = reduceRdd.map{ case ( (x,y),z ) => (x ,(y,z) )}.groupByKey()
    val sortedRdd = countRdd.mapValues(x => x.toList.sortBy(x => -x._2).take(10))
    sortedRdd.take(10).foreach(println)
    val endTime = System.currentTimeMillis
    println("Total Run Time: " + (endTime - startTime) / 1000 + " Sec")
    spark.stop
  }
}
