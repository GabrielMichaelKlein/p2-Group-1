import org.apache.hadoop.fs.FileAlreadyExistsException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark._
import org.apache.spark.sql.functions.{col, to_date, to_timestamp}

import java.util.Date

object Project_2 {
  case class Entry(entryno: Int, ObservationDate: String, Province_State: String, Country_Region: String, Last_Updated: String, Confirmed: Int, Deaths: Int, Recovered: Int)
  def main(args:Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("p3")
      .config("spark.master", "local")
      .config("spark.sql.catalogImplementation","hive")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val sc = spark.sparkContext
    import spark.implicits._


    val covid_data_DF = spark.read.format("csv").option("header", true).load("input/covid_19_data.csv")

    val covid_US_DF = covid_data_DF.filter(covid_data_DF("Country/Region") === "US")
    //covid_US_DF.select(to_date(covid_US_DF("ObservationDate"), "MM/dd/yyyy").alias("ObservationDate")).show()
    var covid_US_DF_clean = covid_US_DF.withColumn("Date", to_date($"ObservationDate", "MM/dd/yyyy"))
    covid_US_DF_clean = covid_US_DF_clean.withColumn("Deaths_Int", covid_US_DF_clean("Deaths").cast(IntegerType)).drop("Deaths")
    covid_US_DF_clean.createOrReplaceTempView("covid_US")
    val df1 = spark.sql("SELECT `Province/State` AS State, MAX(Deaths_INT) AS deaths FROM covid_US WHERE `Province/State` NOT LIKE '%,%' GROUP BY `Province/State` HAVING deaths>50")
    df1.show(200)
    df1.describe().show()

    val rdd1 = sc.parallelize(Seq("Alabama", "Alaska", "Arizona", "Arkansas", "California" ,"" +
      "Colorado", "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "" +
      "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "" +
      "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire", "" +
      "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio", "Oklahoma", "" +
      "Oregon", "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas", "" +
      "Utah", "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming", "District of Columbia"))

    val states_DF = rdd1.toDF("States")
    states_DF.show()
    val records = states_DF.join(covid_US_DF_clean, col("Province/State") === col("States"))
    records.createOrReplaceTempView("records")
    val final_DF = spark.sql("SELECT States, Date, CAST(Confirmed AS INT), Deaths_Int AS Deaths FROM records")

    try {
      final_DF.write.format("com.databricks.spark.csv").save("covid_us_final.csv")

    } catch {
      case _: Throwable => println("EXCEPTION FOUND: FILE ALREADY EXISTS!")
    }
  }
}
