
import org.apache.spark.sql.SparkSession

object BigDataProject {

  def main(args: Array[String]): Unit= {

    // Create a spark session
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Creating 2 Hive tables from spark")
      .enableHiveSupport()
      .getOrCreate()

    //Create a source table and Load data into source table
    spark.sql(s"""CREATE TABLE IF NOT EXISTS src(foo STRING, bar BIGINT, baz DECIMAL, part STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' location '/Users/hassanakbar/Documents/Scala/BigDataProject/src/main/resources/data.csv' """)

    // Display content of source table
    spark.sql("select * from src").show()

    //Create backup partitioned table
    spark.sql(s"""CREATE TABLE IF NOT EXISTS backup(foo STRING, bar BIGINT, baz DECIMAL) PARTITIONED BY (part STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS backup""")

    //Instruct hive to dynamically load partitions
    spark.sql("set hive.exec.dynamic.partition = true")
    spark.sql("set hive.exec.dynamic.partition.mode = nonstrict")

    //Import data into backup partitioned table from source table
    spark.sql("INSERT OVERWRITE TABLE backup PARTITION (part) SELECT * FROM src")

    // Display content of backup table
    spark.sql("SELECT * FROM backup").show()
  }
}
