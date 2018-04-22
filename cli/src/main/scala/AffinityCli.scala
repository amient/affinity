import com.typesafe.config.{Config, ConfigFactory}
import io.amient.affinity.core.config.CfgStruct

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

trait Tool {
  def Conf: CfgStruct[_]

  def cliOptions = Conf.map.asScala.keys.map(o => s"--${o.replace(".", "-")}").mkString(",")

  final def apply(args: List[String]): Unit = apply(args, ConfigFactory.empty)

  def apply(args: List[String], config: Config): Unit
}

object AffinityCli extends App {

  val tools: Map[String, Tool] = Map(
    "timelog" -> TimeLogTool,
    "rebalance" -> RebalanceTool,
    "doc" -> DocTool
  )

  if (args.length == 0) {
    println("Usage: affinity-cli <command> [command-options]\n")
    println("Available commands:")
    tools.foreach {
      case (command, tool) => println(s"\t\t${command.padTo(15, ' ')}\t${tool.Conf.description} [${tool.cliOptions}]")
    }
    sys.exit(2)
  } else try {
    tools(args(0)).apply(args.toList.drop(1))
  } catch {
    case _: scala.MatchError => sys.exit(1)
    case NonFatal(e) =>
      e.printStackTrace()
      sys.exit(2)
  }
}
