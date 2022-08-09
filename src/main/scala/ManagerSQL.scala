import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, desc, max}

object ManagerSQL {

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

    fm_df.createOrReplaceGlobalTempView("premier_league_2000_2022_tmp_view")

    println("The Premier League winners from 2000 to 2022")
    spark.sql("select Season , Team " +
      "from premier_league_2000_2022_tmp_view where Pos = 1").show()

    println("How many times each team won the title?")
    spark.sql("select Team, count(Team) numberOfTitles where Pos = 1 " +
      "group by Team order by numberOfTitles desc").show()

    println("Which teams won the golden title (without any losses)?")
    spark.sql("select Season , Team from where Pos = 1 && L = 0").show()

    println("Ordering the winners points from highest to lowest")
    spark.sql("select Season , Team , Pts where Pos = 1 order by Pts desc").show()

    println("How many times each team qualified to champions?")
    spark.sql("select Team , count(Team) numberOfChampionsQualification " +
        "where Qualification or relegation like \"Qualification for the Champions League%\"" +
        "group by Team order by numberOfChampionsQualification desc").show()

    println("Top goals scored by each team")
    spark.sql("select Season , Team, max(GF) goals_per_season group by Team , Season" +
      "order by goals_per_season desc").show()

    println("Best teams through the 22 years")
    spark.sql("select Season , Team , (GD * Pts) Team_score order by Team_score desc").show()

    spark.stop()
  }


}
