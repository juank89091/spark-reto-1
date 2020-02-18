package retouno

import org.apache.spark.sql.Dataset
import org.scalatest.FunSuite


class challenge1Test extends FunSuite with SparkSessionTestWrapper with Challenge1 {
  import spark.implicits._
  spark.sparkContext.setLogLevel("DEBUG")

  test("delayedAirlines") {

    val csv: Dataset[AirlineDelay] = spark.read.format("csv")
      .option("header", "true")
      .load(
        "resources/delay-and-cancellation/2009.csv",
//        "resources/delay-and-cancellation/2010.csv",
//        "resources/delay-and-cancellation/2011.csv",
//        "resources/delay-and-cancellation/2012.csv",
//        "resources/delay-and-cancellation/2013.csv",
//        "resources/delay-and-cancellation/2014.csv",
//        "resources/delay-and-cancellation/2015.csv",
//        "resources/delay-and-cancellation/2016.csv",
//        "resources/delay-and-cancellation/2017.csv",
        "resources/delay-and-cancellation/2018.csv"
      ).as[AirlineDelay]

    delayedAirlines(csv, None)
  }

}
