package retouno

import org.apache.spark.sql.{DataFrame, Dataset, RelationalGroupedDataset}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.{count, sum}



//1. ¿Cuáles son las aerolíneas más cumplidas y las menos cumplidas de un año en especifico?
//La respuesta debe incluir el nombre completo de la aerolínea, si no se envia el año debe calcular con
//toda la información disponible.

case class AirlineDelay(FL_DATE: String,
                        OP_CARRIER: String,
                        ORIGIN: String,
                        DEST: String,
                        DEP_DELAY: Option[String],
                        ARR_DELAY: Option[String])

case class AirlineStats(name: String,
                        totalFlights: Long,
                        largeDelayFlights: Long,
                        smallDelayFlights: Long,
                        onTimeFlights: Long)

case class FlightsStats(destination: String, morningFlights: Long, afternoonFlights: Long, nightFlights: Long)
case class Flights(ORIGIN: String, DEST: String, DEP_TIME: String)

trait Challenge1 {

  /**
   * Un vuelo se clasifica de la siguiente manera:
   * ARR_DELAY < 5 min --- On time
   * 5 > ARR_DELAY < 45min -- small Delay
   * ARR_DELAY > 45min large delay
   *
   * Calcule por cada aerolinea el número total de vuelos durante el año (en caso de no recibir el parametro de todos los años)
   * y el número de ontime flights, smallDelay flighst y largeDelay flights
   *
   * Orderne el resultado por largeDelayFlights, smallDelayFlightsy, ontimeFlights
   *
   * @param ds
   * @param year
   */
  def delayedAirlines(ds: Dataset[AirlineDelay], year: Option[String]): Seq[AirlineStats] = {

    import org.apache.spark.sql.functions.udf
    import ds.sparkSession.implicits._

    val toUdf = udf(onTime)
    val toUdfSmall = udf(small_delay)
    val toUdfLarge = udf(large_delay)

    val dataSet = year.map(y =>
        ds.filter(a => a.FL_DATE.substring(0, 4) == y))
      .getOrElse(ds)

    val clasificacionVuelo =
      dataSet.filter(_.ARR_DELAY.isDefined)
        .withColumn("onTimeFlights", toUdf(col("ARR_DELAY")))
        .withColumn("smallDelayFlights", toUdfSmall(col("ARR_DELAY")))
        .withColumn("largeDelayFlights", toUdfLarge(col("ARR_DELAY")))
        .groupBy("OP_CARRIER")
        .agg(
          count("OP_CARRIER").as("totalFlights"),
          sum("onTimeFlights").as("onTimeFlights"),
          sum("smallDelayFlights").as("smallDelayFlights"),
          sum("largeDelayFlights").as("largeDelayFlights")
        ).withColumnRenamed("OP_CARRIER", "name")
      .sort(col("largeDelayFlights").desc, col("smallDelayFlights").desc,col("onTimeFlights").desc)
          .as[AirlineStats]

    clasificacionVuelo.show()
    clasificacionVuelo.collect().toList
  }

  def onTime: String => Int = { retraso =>
    if (retraso.toDouble < 5  ) 1 else 0
  }

  def small_delay: String => Int = { retraso =>
    if (retraso.toDouble > 5  && retraso.toDouble < 45 ) 1 else 0
  }

  def large_delay: String => Int = { retraso =>
    if (retraso.toDouble >= 45  ) 1 else 0
  }

  // 2. Dado un origen por ejemplo DCA (Washington), ¿Cuáles son destinos y cuantos vuelos presentan durante la mañana, tarde y noche?

  /**
   * Encuentre los destinos a partir de un origen, y de acuerdo a DEP_TIME clasifique el vuelo de la siguiente manera:
   * 00:00 y 8:00 - Morning
   * 8:01 y 16:00 - Afternoon
   * 16:01 y 23:59 - Night
   *
   * @param ds
   * @param origin
   * @return
   */
  def destinations(ds: Dataset[Flights], origin: String): Seq[FlightsStats] = {
    import ds.sparkSession.implicits._

    ds.filter(f => f.ORIGIN == origin )
      .map(a => FlightsStats(a.DEST,
        if (a.DEP_TIME.toDouble > 0 && a.DEP_TIME.toDouble <= 800) 1 else 0,
        if (a.DEP_TIME.toDouble > 800 && a.DEP_TIME.toDouble <= 1600) 1 else 0,
        if (a.DEP_TIME.toDouble > 1600 && a.DEP_TIME.toDouble <= 2400) 1 else 0
      ))
      .collect().toList
      .groupBy(f => f.destination)
      .map(t => FlightsStats(
        t._1,
        t._2.map(_.morningFlights).foldLeft(0L)(_ + _),
        t._2.map(_.afternoonFlights).foldLeft(0L)(_ + _),
        t._2.map(_.nightFlights).foldLeft(0L)(_ + _))).toList
  }

  //3. Encuentre ¿Cuáles son los números de vuelo (top 20)  que han tenido más cancelaciones y sus causas?

  case class CancelledFlight(number: Int, origin: String, destination: String, cancelled: Long, causes: List[(String, Int)])

  /**
   * Encuentre los vuelos más cancelados y cual es la causa mas frecuente
   * Un vuelo es cancelado si CANCELLED = 1
   * CANCELLATION_CODE A - Airline/Carrier; B - Weather; C - National Air System; D - Security
   *
   * @param ds
   */
  def flightInfo(ds: DataFrame): Seq[CancelledFlight] = ???

  //4. ¿Que dias se presentan más retrasos históricamente?
  /**
   * Encuentre que dia de la semana se presentan más demoras,
   * sólo tenga en cuenta los vuelos donde ARR_DELAY > 45min
   *
   * @param ds
   * @return Una lista con tuplas de la forma (DayOfTheWeek, NumberOfDelays) i.e.("Monday",12356)
   */
  def daysWithDelays(ds: DataFrame): List[(String, Long)] = ???

}