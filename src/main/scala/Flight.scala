import java.io.Serializable

import scala.beans.BeanProperty

class Flight extends Serializable{
  @BeanProperty var Year: String = null
  @BeanProperty var Month: String = null
  @BeanProperty var DayofMonth: String = null
  @BeanProperty var DayOfWeek: String = null
  @BeanProperty var DepTime: String = null
  @BeanProperty var CRSDepTime: String = null
  @BeanProperty var ArrTime: String = null
  @BeanProperty var CRSArrTime: String = null
  @BeanProperty var UniqueCarrier: String = null
  @BeanProperty var FlightNum: String = null
  @BeanProperty var TailNum: String = null
  @BeanProperty var ActualElapsedTime: String = null
  @BeanProperty var CRSElapsedTime: String = null
  @BeanProperty var AirTime: String = null
  @BeanProperty var ArrDelay: String = null
  @BeanProperty var DepDelay: String = null
  @BeanProperty var Origin: String = null
  @BeanProperty var Dest: String = null
  @BeanProperty var Distance: String = null
  @BeanProperty var TaxiIn: String = null
  @BeanProperty var TaxiOut: String = null
  @BeanProperty var Cancelled: String = null
  @BeanProperty var CancellationCode: String = null
  @BeanProperty var Diverted: String = null
  @BeanProperty var CarrierDelay: String = null
  @BeanProperty var WeatherDelay: String = null
  @BeanProperty var NASDelay: String = null
  @BeanProperty var SecurityDelay: String = null
  @BeanProperty var LateAircraftDelay: String = null
}
