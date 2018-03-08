import com.indvd00m.ascii.render.{Point, Region, Render}
import com.indvd00m.ascii.render.api.{ICanvas, IContextBuilder, IRender}
import com.indvd00m.ascii.render.elements.{Dot, Label, Line, Rectangle}
import com.indvd00m.ascii.render.elements.plot.{Axis, AxisLabels}
import com.typesafe.config.ConfigFactory
import io.amient.affinity.core.storage.{LogEntry, LogStorage, LogStorageConf}
import io.amient.affinity.core.util.{EventTime, TimeRange}
import io.amient.affinity.kafka.KafkaLogStorage
import io.amient.affinity.kafka.KafkaStorage.KafkaStorageConf

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object TimeLogUtil {

  val width = 180
  val height = 41

  private var minTimestamp = Long.MaxValue
  private var maxTimestamp = Long.MinValue
  private var maxPosition = Long.MinValue
  private var minPosition = Long.MaxValue
  private var numRecords = 0
  private val blocks = ListBuffer[(TimeRange, Long, Long)]()
  private var plotted = false

  def apply(args: List[String]): Unit = args match {
    case bootstrap :: topic :: partition :: fuzz :: fromDT :: toDT :: Nil => apply(bootstrap, topic, partition.toInt, fuzz.toInt, new TimeRange(fromDT, toDT))
    case bootstrap :: topic :: partition :: fuzz :: fromDT :: Nil => apply(bootstrap, topic, partition.toInt, fuzz.toInt, TimeRange.since(fromDT))
    case bootstrap :: topic :: partition :: fuzz :: Nil => apply(bootstrap, topic, partition.toInt, fuzz.toInt)
    case bootstrap :: topic :: partition :: Nil => apply(bootstrap, topic, partition.toInt)
    case bootstrap :: topic :: Nil => apply(bootstrap, topic)
    case _ => printHelp()
  }

  def printHelp(): Unit = {
    println("Usage: timelog <kafka-bootstrap> <topic> [<partition> [<resolution-minutes>] [<from-datetime> [<to-datetime>]]]\n")
  }

  def apply(bootstrap: String, topic: String): Unit = {
    println("Available partitions: 0 - " + (getKafkaLog(bootstrap, topic).getNumPartitions-1))
  }

  def apply(bootstrap: String, topic: String, partition: Int, fuzzMinutes: Long = 5, range: TimeRange = TimeRange.UNBOUNDED) {
    val log = getKafkaLog(bootstrap, topic)
    println(s"calculating compaction stats for range: $range..\n")
    log.reset(partition, range)
    var blockmints = Long.MaxValue
    var blockmaxts = Long.MinValue
    var startpos = -1L
    var endpos = -1L
    var lastts = Long.MinValue
    def addblock(): Unit = {
      val timerange: TimeRange = new TimeRange(blockmints, blockmaxts)
      blocks += ((timerange, startpos, endpos))
      println(s"Block $startpos : $endpos -> $timerange")
      startpos = -1L
      endpos = -1L
      blockmaxts = Long.MinValue
      blockmints = Long.MaxValue
      lastts = Long.MinValue
    }
    def maybeAddBlock(entry: LogEntry[java.lang.Long]): Unit = {
      if (lastts == Long.MinValue) return
      if (entry.timestamp > lastts - fuzzMinutes * 60000 && entry.timestamp < lastts + fuzzMinutes * 60000) return
      addblock()
    }
    log.boundedIterator.asScala.foreach {
      entry =>
        maybeAddBlock(entry)
        if (startpos == -1) startpos = entry.position
        minPosition = math.min(minPosition, entry.position)
        maxPosition = math.max(maxPosition, entry.position)
        endpos = entry.position
        lastts = entry.timestamp
        blockmints = math.min(blockmints, entry.timestamp)
        blockmaxts = math.max(blockmaxts, entry.timestamp)
        minTimestamp = math.min(minTimestamp, entry.timestamp)
        maxTimestamp = math.max(maxTimestamp, entry.timestamp)
        numRecords += 1
    }
    if (startpos > -1) addblock()
    println("number of records: " + numRecords)
    println("minimum timestamp: " + pretty(minTimestamp))
    println("maximum timestamp: " + pretty(maxTimestamp))
    println("minimum offset: " + minPosition)
    println("maximum offset: " + maxPosition)
    plot(blocks.toList)
  }

  private def getKafkaLog(bootstrap: String, topic: String): KafkaLogStorage = {
    println(s"initializing $bootstrap / $topic")
    val conf = new LogStorageConf().apply(ConfigFactory.parseMap(Map(
      LogStorage.StorageConf.Class.path -> classOf[KafkaLogStorage].getName(),
      KafkaStorageConf.BootstrapServers.path -> bootstrap,
      KafkaStorageConf.Topic.path -> topic
    ).asJava))
// TODO this type of configuration should also work:
//    conf.Class.setValue(classOf[KafkaLogStorage])
//    KafkaStorageConf(conf).BootstrapServers.setValue(bootstrap)
//    KafkaStorageConf(conf).Topic.setValue(topic)
    LogStorage.newInstance(conf).asInstanceOf[KafkaLogStorage]
  }

  private def pretty(unix: Long): String = {
    EventTime.local(unix).toString.replace("Z", "").replace("T", " ")
  }

  private def plot(blocks: List[(TimeRange, Long, Long)]) {
    if (plotted) print(String.format("\033[2J"))
    val render: IRender = new Render
    val builder: IContextBuilder = render.newBuilder
    builder.width(width).height(height)
    val xratio = width.toDouble / (maxTimestamp - minTimestamp)
    val yratio = height.toDouble / (maxPosition - minPosition)
    blocks.foreach {
      case (timerange, startpos, endpos) =>
        val x = ((timerange.start - minTimestamp) * xratio).toInt
        val y = height - ((endpos - minPosition) * yratio).toInt
        val w = math.max(0, ((timerange.end - timerange.start) * xratio).toInt)
        val h = math.max(0, ((endpos - startpos) * yratio).toInt)
        if (w < 2 || h < 2) {
          builder.element(new Line(new Point(x, y), new Point(x + w, y + h)))
        } else {
          builder.element(new Rectangle(x, y, w, h))
          if (w > 20) {
            builder.element(new Label(pretty(timerange.end).toString, x + w - 20, y + 1))
            if (h > 3) {
              builder.element(new Label(endpos.toString.reverse.padTo(19, ' ').reverse, x + w - 20, y + 2))
            }
            if (w > 42 || h > 4) {
              builder.element(new Label(startpos.toString, x + 1, y + h - 3))
              if (h > 1) {
                builder.element(new Label(pretty(timerange.start).toString, x + 1, y + h - 2))
              }
            } else if (h > 3) {
              builder.element(new Label(startpos.toString, x + 1, y + h - 2))
            }
          }

        }
    }
    val canvas: ICanvas = render.render(builder.build)
    println(canvas.getText)
  }

}
