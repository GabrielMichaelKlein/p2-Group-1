/*************************************************
 *  Gabriel Klein, Grant Muse, Jesse Sabbath
 *  p2
 *  Group 1
 *
 * This project explores trends in various Covid-19 data
 * from across the world using Spark.
 ************************************************/

import org.apache.hadoop.fs.FileAlreadyExistsException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark._
import org.apache.spark.sql.functions.{asc, col, desc, lit, round, sum, to_date, to_timestamp}

import java.util.Date
import scala.Console.println

object Project_2 {
  // Case classes for DF creation
  case class Entry(entryno: Int, ObservationDate: String, Province_State: String, Country_Region: String, Last_Updated: String, Confirmed: Int, Deaths: Int, Recovered: Int)
  case class Region(UID : String, iso2 : String, iso3 : String, code3 : String, FIPS : String, Admin2 : String, Province : String, Country_Region : String, Lat : String, Long_ : String, total : Int)
  case class Region2(UID : String, iso2 : String, iso3 : String, code3 : String, FIPS : String, Admin2 : String, Province : String, Country_Region : String, Lat : String, Long_ : String, Population : Int, Deaths : Int)

  def grantsPart(spark:SparkSession): Unit ={
    /*
    This method searches through time_series_covid_19_confirmed_US.csv and time_series_covid_19_deaths_USnew.csv,
    relates the cases and deaths due to covid to the population of each state, and creates a new csv file
    which can be imported into excel to create a bar chart showing the trends from each state.
     */
    println("Running Grant's Part")
    import spark.implicits._
    println("generating dataframes")
    val rddFromFile = spark.sparkContext.textFile("input/time_series_covid_19_confirmed_US.csv")
    val rddConfirmed = rddFromFile.map(_.split(","))
    val dfConfirmed = rddConfirmed.filter(_(0)!="UID").map(a => Region(a(0),a(1),a(2),a(3),a(4),a(5),a(6),a(7),a(8),a(9), a(a.length-1).toInt)).toDF
    val dfTotalConfirmed = dfConfirmed.groupBy("Province").agg(sum("total").as("totalConfirmed")).orderBy("Province")

    val rddFromFile2 = spark.sparkContext.textFile("input/time_series_covid_19_deaths_USnew.csv")
    val rddDeath = rddFromFile2.map(_.split( ","))
    val dfDeath = rddDeath.filter(_(0)!="UID").map(a => Region2(a(0), a(1), a(2), a(3), a(4), a(5), a(6), a(7), a(8), a(9), a(10).toInt, a(a.length-1).toInt)).toDF
    println("[success] generating dataframes")
    val dfTotalPopulation = dfDeath.groupBy("Province").agg(sum("Population").alias("totalPop"), sum("deaths").alias("totalDeaths")).orderBy("Province")

    dfTotalPopulation.createOrReplaceTempView("population")
    dfTotalConfirmed.createOrReplaceTempView("confirmed")

    val percentConfirmedTable = spark.sql("SELECT confirmed.province as state, totalConfirmed, totalDeaths, totalPop, ((totalConfirmed / totalPop) * 100) as confirmedOverPop, ((totalDeaths / totalPop)*100) as deathsOverPop, ((totalDeaths / totalConfirmed)*100) as deathsOverConfirmed " +
      "FROM confirmed INNER JOIN population ON confirmed.province = population.province ORDER BY state desc")

    // Repartitioned DF that can be written to a CSV file
    val percentConfirmedTable_toCSV = percentConfirmedTable.repartition(1)

    try {
      percentConfirmedTable_toCSV.write.format("com.databricks.spark.csv").save("percentage_by_pop.csv")
    } catch {
      case _: Throwable => println("EXCEPTION FOUND: FILE ALREADY EXISTS!")
    }
    percentConfirmedTable.show(60)
    /* Extra Queries
    percentConfirmedTable.select("state","confirmedOverPop").orderBy(desc("confirmedOverPop")).limit(1).show
    percentConfirmedTable.select("state","deathsOverPop").orderBy(desc("deathsOverPop")).limit(1).show
    percentConfirmedTable.select("state","deathsOverConfirmed").where("totalConfirmed > 5000").orderBy(desc("deathsOverConfirmed")).limit(1).show

    percentConfirmedTable.select("state","confirmedOverPop").where("totalConfirmed > 5000").orderBy(asc("confirmedOverPop")).limit(1).show
    percentConfirmedTable.select("state","deathsOverPop").where("totalConfirmed > 5000").orderBy(asc("deathsOverPop")).limit(1).show
    percentConfirmedTable.select("state","deathsOverConfirmed").where("totalConfirmed > 5000").orderBy(asc("deathsOverConfirmed")).limit(1).show
    */
  }

  def covid_US_Trends(spark:SparkSession): Unit = {
    /*
    This method takes in covid_19_data.csv, filters out everything except for US data, and further
    cleans it up by removing erroneous data. It then creates a new csv file that can then be imported
    into excel to create a graph showing the covid trends in the US over time.
     */
    spark.sparkContext.setLogLevel("WARN")
    val sc = spark.sparkContext
    import spark.implicits._

    val covid_data_DF = spark.read.format("csv").option("header", true).load("input/covid_19_data.csv")

    val covid_US_DF = covid_data_DF.filter(covid_data_DF("Country/Region") === "US")
    var covid_US_DF_clean = covid_US_DF.withColumn("Date", to_date($"ObservationDate", "MM/dd/yyyy"))
    covid_US_DF_clean = covid_US_DF_clean.withColumn("Deaths_Int", covid_US_DF_clean("Deaths").cast(IntegerType)).drop("Deaths")

    // RDD that holds the names of all the states, plus Washington DC
    val states = sc.parallelize(Seq("Alabama", "Alaska", "Arizona", "Arkansas", "California" ,"" +
      "Colorado", "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "" +
      "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "" +
      "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire", "" +
      "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio", "Oklahoma", "" +
      "Oregon", "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas", "" +
      "Utah", "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming", "District of Columbia"))

    val states_DF = states.toDF("States")
    states_DF.cache()
    val records = states_DF.join(covid_US_DF_clean, col("Province/State") === col("States"))
    records.createOrReplaceTempView("records")
    val final_DF = spark.sql("SELECT States, Date, CAST(Confirmed AS INT), Deaths_Int AS Deaths FROM records")

    println("Covid cases/deaths over time by state: ")
    final_DF.show()

    // Try-catch as write will throw an exception if file already exists
    try {
      final_DF.write.format("com.databricks.spark.csv").save("covid_us_final.csv")
    } catch {
      case _: Throwable => println("EXCEPTION FOUND: FILE ALREADY EXISTS!")
    }
  }

  def covid_Global_Trends(spark:SparkSession): Unit = {
    /*
    This method searches through time_series_covid_19_confirmed.csv and time_series_covid_19_deaths.csv,
    looks at the cases and deaths of every country, and creates a csv with cleaned up and filtered data which
    can be used by Excel to create graphs to visualize the deaths per infection in different countries.
     */
    val casesDF =  spark.read.options(Map("inferSchema"->"true", "header"->"true")).csv("input/time_series_covid_19_confirmed.csv")
    val deathsDF =  spark.read.options(Map("inferSchema"->"true", "header"->"true")).csv("input/time_series_covid_19_deaths.csv")

    //Construct cases and death DFs, grouping by country
    val deathsQuarterlyUngroupedDF = deathsDF.select(col("Country/Region"),col("2/2/20").as("D 2/20"), col("5/2/20").as("D 5/20"),col("8/2/20").as("D 8/20"), col("11/2/20").as("D 11/20"), col("2/20/21").as("D 2/21"),col("5/2/21").as("D 5/21"))
    val deathsByCountryDF = deathsQuarterlyUngroupedDF
      .groupBy("Country/Region")//.sum("D 5/21")
      .agg(sum("D 2/20"), sum("D 5/20"), sum ("D 8/20"), sum("D 11/20"), sum("D 2/21"), sum("D 5/21"))
      .orderBy(col("sum(D 5/21)").desc)

    val casesQuarterlyUngroupedDF = casesDF.select(col("Country/Region"),col("2/2/20").as("C 2/20"), col("5/2/20").as("C 5/20"),col("8/2/20").as("C 8/20"), col("11/2/20").as("C 11/20"), col("2/20/21").as("C 2/21"),col("5/2/21").as("C 5/21"))
    var casesByCountryDF = casesQuarterlyUngroupedDF
      .groupBy("Country/Region")//.sum("C 5/21")
      .agg(sum ("C 2/20"), sum("C 5/20"), sum ("C 8/20"), sum("C 11/20"), sum("C 2/21"), sum("C 5/21"))
      .orderBy(col("sum(C 5/21)").desc)

    casesByCountryDF = casesByCountryDF.withColumnRenamed("Country/Region", "Country")

    //Join cases and deaths DFs
    val joinedData =  deathsByCountryDF
      .join(casesByCountryDF, deathsByCountryDF("Country/Region") === casesByCountryDF("Country"))
      .drop("Country/Region")
      .persist

    //Calculate and sort by death/case ratio
    val deathCaseRatio = joinedData
      .select("Country", "sum(C 5/21)", "sum(D 5/21)")
      .withColumn("Death_Case_Ratio", round(col("sum(D 5/21)")/col("sum(C 5/21)") * 100, 2))
      .orderBy(col("Death_Case_Ratio").desc)

    //Rename column to prevent issues with / in query
    val renamed = joinedData.withColumnRenamed("sum(D 5/21)", "Deaths_May2021").withColumnRenamed("sum(C 5/21)", "Cases_May2021")
    renamed.createOrReplaceTempView("joined_Data")

    //Create new row with global death/case ratio and add it to the ratio df via union
    val globalRatioDF = spark.sql("Select sum(Cases_May2021) as Cases_May2021, sum(Deaths_May2021) as Deaths_May2021, round(sum(Deaths_May2021)/sum(Cases_May2021), 2) * 100 as Death_Case_Ratio from joined_Data")
    val withCountry = globalRatioDF.withColumn("Country", lit("Global"))

    val deathCaseRatioFinalDF = withCountry.select("Country","Cases_May2021", "Deaths_May2021", "Death_Case_Ratio")
      .union(deathCaseRatio.select("Country", "sum(C 5/21)", "sum(D 5/21)", "Death_Case_Ratio"))
      .orderBy(col("Death_Case_Ratio").desc)

    //Print trends
    println("\nCases by Country: 5/2/20 to 5/2/21")
    casesByCountryDF.show

    println("\nDeaths by Country: 5/2/20 to 5/2/21")
    deathsByCountryDF.show

    println("\nDeath_Case_Ratios: 5/2/21")
    deathCaseRatioFinalDF.show

    println("\nGlobal vs US Rates as of 5/2/21: ")
    deathCaseRatioFinalDF.select("Country","Cases_May2021", "Deaths_May2021", "Death_Case_Ratio")
      .where(deathCaseRatioFinalDF("Country") === "US" || deathCaseRatioFinalDF("Country") === "Global").show

    //Compile to one visualizationDF for .csv export
    val tempRatioDF = deathCaseRatioFinalDF.withColumnRenamed("Country","C2")
    val visualizationDF = joinedData.join(tempRatioDF.select("C2","Death_Case_Ratio"), tempRatioDF("C2") === joinedData("Country")).drop("C2")

    try {
      visualizationDF.coalesce(1).write
        .option("header","true")
        .format("com.databricks.spark.csv")
        .save("visualization.csv")
    }
    catch {
      case _: Throwable => println("EXCEPTION FOUND: FILE ALREADY EXISTS!")
    }
  }


  def main(args:Array[String]): Unit = {
    /*
    Main function. Initializes the SparkSession and runs the various methods that explore different trends
     */
    val spark = SparkSession
      .builder
      .appName("p3")
      .config("spark.master", "local")
      .config("spark.sql.catalogImplementation", "hive")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    grantsPart(spark)
    covid_US_Trends(spark)
    covid_Global_Trends(spark)
  }
}
