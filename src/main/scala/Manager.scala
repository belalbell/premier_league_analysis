import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._

object Manager {

  val fileName = "EPL_standings_2000-2022.csv"

  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val fm_df = (spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(fileName)).cache()

    fm_df.printSchema()

    println("The Premier League winners from 2000 to 2022")
    val winners = fm_df.select("Season" , "Team")
      .where("Pos= 1").show()

    println("How many times each team won the title?")
    val winner_count = fm_df.select("Team")
      .where("Pos=1")
      .groupBy("Team")
      .agg(count("Team").alias("numberOfTitles"))
      .orderBy(desc("numberOfTitles")).show()

    println("Which teams won the golden title (without any losses)?")
    val winner_without_loss = fm_df.select("Season" , "Team")
      .where("Pos = 1 and L = 0").show()

    println("Ordering the winners points from highest to lowest")
    val winner_points = fm_df.select("Season" , "Team", "Pts")
      .where("Pos = 1")
      .orderBy(desc("Pts")).show()

    println("How many times each team qualified to champions?")
    val teams_champions = fm_df.select("Team")
      .where(col("Qualification or relegation") like "Qualification for the Champions League%")
      .groupBy("Team")
      .agg(count("Team").alias("numberOfChampionsQualification"))
      .orderBy(desc("numberOfChampionsQualification")).show()

    println("Top goals scored by each team")
    val teams_goals_seasons = fm_df.select("Season" , "Team" , "GF")
      .groupBy("Season", "Team")
      .agg(max("GF").alias("goals_per_season"))
      .orderBy(desc("goals_per_season")).show()

    println("Best teams through the 22 years")
    val teams_ff = fm_df.select(col("Season") ,col("Team"), (col("GD") * col("Pts")).alias("best_ratio"))
      .orderBy(desc("best_ratio")).show(10)


    spark.stop()
  }

}
