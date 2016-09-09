import java.io.Serializable

import scala.beans.BeanProperty

class Superhero extends Serializable {
  @BeanProperty var year: Int = 0
  @BeanProperty var title: String = null
  @BeanProperty var marvelDC: String = null
  @BeanProperty var IMDB: Double = 0.0
  @BeanProperty var rottenTomatoes: Int = 0
  @BeanProperty var IMDBRottenTomatoesComposite: Double = 0.0
  @BeanProperty var openingWeekendBoxOffice: Double = 0.0
  @BeanProperty var avgMovieTicketPriceForYear: Double = 0.0
  @BeanProperty var estOpeningWeekendAttendance: Double = 0.0
  @BeanProperty var USPopulationYearOfOpening: Double = 0.0
  @BeanProperty var percentOfPopulationAttendingOpeningWeekend: Double = 0.0
}