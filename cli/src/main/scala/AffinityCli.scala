import scala.util.control.NonFatal

object AffinityCli extends App {

  try args(0) match {
    case "timelog" => TimeLogUtil(args.toList.drop(1))
  } catch {
    case _: scala.MatchError => sys.exit(1)
    case NonFatal(e) =>
      e.printStackTrace()
      sys.exit(2)
  }
}
