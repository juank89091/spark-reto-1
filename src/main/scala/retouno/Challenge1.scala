package retouno

import org.apache.spark.sql.{DataFrame, Dataset, RelationalGroupedDataset}
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
    val dataSet =
      year.map(y =>
        ds.filter(a => a.FL_DATE.substring(0, 4) == y))
      .getOrElse(ds)

    val numeroVuelosAerolinea: DataFrame = dataSet.groupBy("OP_CARRIER")

      .count()

    numeroVuelosAerolinea.show()

  //  dataSet.filter(ad => ad.ARR_DELAY.isDefined)
  //      .map(ad => ad)


null  }

  // 2. Dado un origen por ejemplo DCA (Washington), ¿Cuáles son destinos y cuantos vuelos presentan durante la mañana, tarde y noche?
  case class FlightsStats(destination: String, morningFlights: Long, afternoonFlights: Long, nightFlights: Long)

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
  def destinations(ds: DataFrame, origin: String): Seq[FlightsStats] = ???

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