import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import io.amient.affinity.core.config.CfgStruct

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

trait Tool {
  def Conf: CfgStruct[_]

  def cliOptions = Conf.map.asScala.keys.map(cliArg).mkString(",")

  def cliArg(propertyName: String): String = s"--${propertyName.replace(".", "-")}"

  def propKey(cli: String): String = cli.drop(2).replace("-", ".")

  def apply(config: Config): Unit

  final def apply(args: List[String]): Unit = apply(args, ConfigFactory.empty)

  def apply(args: List[String], config: Config): Unit = {
    val cliKeys = Conf.map.asScala.keys.map(prop => cliArg(prop) -> prop).toMap
    args match {
      case cliArg :: cliValue :: tail if cliKeys.contains(cliArg) =>
        apply(tail, config.withValue(propKey(cliArg), ConfigValueFactory.fromAnyRef(cliValue)))
      case Nil => apply(config)
      case options => throw new IllegalArgumentException(s"Unknown command arguments: ${options.mkString(" ")}")
    }
  }

}

object AffinityCli extends App {

  val tools: Map[String, Tool] = Map(
    "timelog" -> TimeLogTool,
    "rebalance" -> RebalanceTool,
    "doc" -> DocTool
  )

try {
    if (args.length == 0) throw new IllegalArgumentException("Missing command argument")
    val command = args(0)
    val tool = tools(command)
    try {
      tool.apply(args.toList.drop(1))
    } catch {
      case e: IllegalArgumentException =>
        println(e.getMessage)
        println(s"Usage: affinity-cli $command [arguments]")
        println("\nRequired arguments: ")
        tool.Conf.map.asScala.filter(_._2.isRequired).foreach {
          case (name, cfg) =>
            println(s"\t\t${(tool.cliArg(name) + " <"+cfg.parameterInfo+">").padTo(40, ' ')}\t${cfg.description}")
        }
        println("\nOptional arguments: ")
        tool.Conf.map.asScala.filter(!_._2.isRequired).foreach {
          case (name, cfg) =>
            println(s"\t\t${(tool.cliArg(name) + " <"+cfg.parameterInfo+">").padTo(40, ' ')}\t${cfg.description}")
        }
    }
  } catch {
    case e: IllegalArgumentException =>
      println(e.getMessage)
      println("\nUsage: affinity-cli <command> [arguments]")
      println("\nAvailable commands:")
      tools.foreach {
        case (command, tool) => println(s"\t\t${command.padTo(15, ' ')}\t${tool.Conf.description} [${tool.cliOptions}]")
      }
      sys.exit(1)
    case NonFatal(e) =>
      e.printStackTrace()
      sys.exit(2)
  }
}
