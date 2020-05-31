import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object MO_Analyzer {

  def main(args: Array[String]): Unit = {
    //val logFile = "C:/Spark/readme.md"
    val conf = new SparkConf().setAppName("CounterDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().appName(name = "TestApp").getOrCreate()
    val dataFile = spark.read.format("com.databricks.spark.csv")
      .option("header", true).load("E:\\IdeaProjects\\Lab10\\russian_demography.csv")
    dataFile.createOrReplaceTempView("Demography")
    // Рождаемости/смертности по МО
    spark.sql("SELECT year, birth_rate, death_rate FROM Demography WHERE region='Moscow Oblast'").show()
    // Средняя рождаемость в МО в первую декаду 21 века
    spark.sql("SELECT AVG(birth_rate) FROM Demography WHERE region='Moscow Oblast' AND year<2010 AND year>=2000").show()
    spark.stop()
  }
}
