package eisber.asa

import org.apache.spark.sql.DataFrame
// import eisber.Sample1
import scala.collection.Map

class ASA {

    def Run(query:String, inputs:Map[String, DataFrame]) = {
        println(query)

        for ((name, df) <- inputs) {
            println(name)
            df.show
        }

        // TODO: remove scala code?
        // should be able to invoke the code directly from ADB

        // TODO: Scala dataframe pass to Java dataframe

        // df.mapPartitions((Iterable[Row] => 
            // val asa = new Sample1()
            // asa.setInputSchema()
            // asa.getOutputSchema()
            // asa.feedRow()
            // asa.getRow()
            // make it sync?
        // ))
        // create Sample1 Java object
        // in constructor load .NET
        // create methods to start new row
        // void setInputSchema(string[] names, string?[] types)
        // (string[], string[] types) getOutputSchema()
        // void feedRow(object[] values);
        // void setOutputCallback(func<object[]> x) //
        // Sample1.run
    }
}