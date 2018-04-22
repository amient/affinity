import com.typesafe.config.{Config, ConfigFactory}
import io.amient.affinity.core.config.{Cfg, CfgStruct}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

trait Tool {
  def apply(args: List[String], config: Config = ConfigFactory.empty): Unit

  def Conf: CfgStruct[_]

  def cliOptions = Conf.map.asScala.keys.map(o => s"--${o.replace(".", "-")}").mkString(",")
}

object AffinityCli extends App {

  val tools: Map[String, Tool] = Map(
    "rebalance" -> RebalancerTool
  )

  if (args.length == 0) {
    println("Usage: affinity-cli <command> [command-options]\n")
    println("Available commands:")
    println("\t\ttimelog\t\t\tUtility for analyzing log compaction ")
    tools.foreach {
      case (command, tool) => println(s"\t\t$command\t\t\t${tool.Conf.description} [${tool.cliOptions}]")
    }

    sys.exit(2)
  } else try args(0) match {
    case "timelog" => TimeLogTool(args.toList.drop(1))
    case "rebalance" => RebalancerTool(args.toList.drop(1))
  } catch {
    case _: scala.MatchError => sys.exit(1)
    case NonFatal(e) =>
      e.printStackTrace()
      sys.exit(2)
  }
}
