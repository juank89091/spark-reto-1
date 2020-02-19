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
        "resources/delay-and-cancellation/2018.csv"
      ).as[AirlineDelay]

    delayedAirlines(csv, None)
  }

  test("destinations") {

    val csv: Dataset[Flights] = spark.read.format("csv")
      .option("header", "true")
      .load(
        "resources/delay-and-cancellation/2009.csv",
        "resources/delay-and-cancellation/2018.csv"
      ).as[Flights]

    destinations(csv, "DCA" )
  }

}
