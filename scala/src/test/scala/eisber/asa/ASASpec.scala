package eisber.asa

import org.scalatest._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.collection.Map
import com.microsoft.AzureStreamAnalytics

class ASASpec extends FlatSpec {

    "asa" should "run query" in  {
        var spark = SparkSession.builder()
            .master("local[2]") // 2 ... number of threads
            .appName("ASA")
            .config("spark.sql.shuffle.partitions", value = 1)
            .config("spark.ui.enabled", value = false)
            .config("spark.sql.crossJoin.enabled", value = true)
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        val df = spark.sqlContext
            .read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("timestampFormat", "MM-dd-yyyy hh mm ss")
            .load(getClass.getResource("/dataset1.csv").getPath)
            .repartition(col("category"))
            .sortWithinPartitions(col("ts"))
            .select(col("ts").cast(LongType), col("category"), col("value"))

        df.show

        // new ASA().run("SELECT * FROM df", Map("df" -> df))
        // new Sample1().intMethod(1)
        // ASAExectuctor.
        // TODO: policy
        // invoked per physical partitions
        val newDf = AzureStreamAnalytics.execute(
            "SELECT category, System.Timestamp AS ts, COUNT(*) AS n FROM input TIMESTAMP BY DATEADD(second, ts, '2019-11-20T00:00:00Z') GROUP BY TumblingWindow(second, 2), category", 
            df)
        // ASAExecutor_run(sql, {'i1': df1, 'i2': df2})
        // df.join(df2, 'left').filter().select()
        // spark.sql(' ... ') , saveAsTable(), createTemporaryView()

        // - schema input/output
        // - additional type support for fields: long, int, datetime [input? output?]
        
        // struct?
        // array
        // - packaging (include CLR (dotnet core) in .jar)
        // nuget?
        // github repo?
        // github.com/Microsoft/ASA4J ?
        // github.com/Azure/ASA4J

        // 2min video what this does. the scenario, power point

        // Spark multiple nodes, good amount of data to show case

        // Spark Summit? April 20

        newDf.show
    }

    /*"asa spark" should "run dotent spark" in {
      var spark = SparkSession.builder()
            .master("local[2]") // 2 ... number of threads
            .appName("ASA")
            .config("spark.sql.shuffle.partitions", value = 1)
            .config("spark.ui.enabled", value = false)
            .config("spark.sql.crossJoin.enabled", value = true)
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        val df = spark.sqlContext
            .read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(getClass.getResource("/dataset1.csv").getPath)
            .repartition(col("category")) // numPartitions:
            .sortWithinPartitions(col("ts"))
            .select(col("ts").cast(LongType), col("category"), col("value"))

        df.show;

        val newDf = AzureStreamAnalytics.executeDotnet(
            "SELECT category, System.Timestamp AS ts, COUNT(*) AS n FROM input TIMESTAMP BY DATEADD(second, ts, '1970-01-01T00:00:00Z') GROUP BY TumblingWindow(second, 2), category",
            df)

    }*/
}
