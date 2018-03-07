package io.amient.affinity.avro.record

import java.io.IOException
import java.io.OutputStream
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util

import org.apache.avro.AvroTypeException
import org.apache.avro.Schema
import org.apache.avro.io.ParsingEncoder
import org.apache.avro.io.parsing.JsonGrammarGenerator
import org.apache.avro.io.parsing.Parser
import org.apache.avro.io.parsing.Symbol
import org.apache.avro.util.Utf8
import org.codehaus.jackson.JsonEncoding
import org.codehaus.jackson.JsonFactory
import org.codehaus.jackson.JsonGenerator
import org.codehaus.jackson.util.DefaultPrettyPrinter
import org.codehaus.jackson.util.MinimalPrettyPrinter


/** An {@link Encoder} for Avro's JSON data encoding.
  * </p>
  * Construct using {@link EncoderFactory}.
  * </p>
  * JsonEncoder buffers output, and data may not appear on the output
  * until {@link Encoder#flush()} is called.
  * </p>
  * JsonEncoder is not thread-safe.
  * */
object JsonEncoder {
  private val LINE_SEPARATOR = System.getProperty("line.separator")

  // by default, one object per line.
  // with pretty option use default pretty printer with root line separator.
  @throws[IOException]
  private def getJsonGenerator(out: OutputStream, pretty: Boolean) = {
    if (null == out) throw new NullPointerException("OutputStream cannot be null")
    val g = new JsonFactory().createJsonGenerator(out, JsonEncoding.UTF8)
    if (pretty) {
      val pp = new DefaultPrettyPrinter() { //@Override
        @throws[IOException]
        override def writeRootValueSeparator(jg: JsonGenerator): Unit = {
          jg.writeRaw(LINE_SEPARATOR)
        }
      }
      g.setPrettyPrinter(pp)
    }
    else {
      val pp = new MinimalPrettyPrinter
      pp.setRootValueSeparator(LINE_SEPARATOR)
      g.setPrettyPrinter(pp)
    }
    g
  }
}

class JsonEncoder private[io](val sc: Schema, out: JsonGenerator) extends ParsingEncoder with Parser.ActionHandler {

  private val parser = new Parser(new JsonGrammarGenerator().generate(sc), this)

  /**
    * Has anything been written into the collections?
    */
  protected var isEmpty: util.BitSet = new util.BitSet()

  def this(sc: Schema, out: OutputStream) {
    this(sc, JsonEncoder.getJsonGenerator(out, false))
  }

  def this(sc: Schema, out: OutputStream, pretty: Boolean) {
    this(sc, JsonEncoder.getJsonGenerator(out, pretty))
  }

  @throws[IOException]
  override def flush(): Unit = {
    parser.processImplicitActions()
    if (out != null) {
      out.flush()
    }
  }

  @throws[IOException]
  override def writeNull(): Unit = {
    parser.advance(Symbol.NULL)
    out.writeNull()
  }

  @throws[IOException]
  override def writeBoolean(b: Boolean): Unit = {
    parser.advance(Symbol.BOOLEAN)
    out.writeBoolean(b)
  }

  @throws[IOException]
  override def writeInt(n: Int): Unit = {
    parser.advance(Symbol.INT)
    out.writeNumber(n)
  }

  @throws[IOException]
  override def writeLong(n: Long): Unit = {
    parser.advance(Symbol.LONG)
    out.writeNumber(n)
  }

  @throws[IOException]
  override def writeFloat(f: Float): Unit = {
    parser.advance(Symbol.FLOAT)
    out.writeNumber(f)
  }

  @throws[IOException]
  override def writeDouble(d: Double): Unit = {
    parser.advance(Symbol.DOUBLE)
    out.writeNumber(d)
  }

  @throws[IOException]
  override def writeString(utf8: Utf8): Unit = {
    writeString(utf8.toString)
  }

  @throws[IOException]
  override def writeString(str: String): Unit = {
    parser.advance(Symbol.STRING)
    if (parser.topSymbol eq Symbol.MAP_KEY_MARKER) {
      parser.advance(Symbol.MAP_KEY_MARKER)
      out.writeFieldName(str)
    }
    else {
      out.writeString(str)
    }
  }

  @throws[IOException]
  override def writeBytes(bytes: ByteBuffer): Unit = {
    if (bytes.hasArray) {
      writeBytes(bytes.array, bytes.position, bytes.remaining)
    }
    else {
      val b: Array[Byte] = new Array[Byte](bytes.remaining)
      bytes.duplicate.get(b)
      writeBytes(b)
    }
  }

  @throws[IOException]
  override def writeBytes(bytes: Array[Byte], start: Int, len: Int): Unit = {
    parser.advance(Symbol.BYTES)
    writeByteArray(bytes, start, len)
  }

  @throws[IOException]
  private def writeByteArray(bytes: Array[Byte], start: Int, len: Int): Unit = {
    out.writeString(new String(bytes, start, len, StandardCharsets.UTF_8))
  }

  @throws[IOException]
  override def writeFixed(bytes: Array[Byte], start: Int, len: Int): Unit = {
    parser.advance(Symbol.FIXED)
    val top: Symbol.IntCheckAction = parser.popSymbol.asInstanceOf[Symbol.IntCheckAction]
    if (len != top.size) {
      throw new AvroTypeException("Incorrect length for fixed binary: expected " + top.size + " but received " + len + " bytes.")
    }
    writeByteArray(bytes, start, len)
  }

  @throws[IOException]
  override def writeEnum(e: Int): Unit = {
    parser.advance(Symbol.ENUM)
    val top: Symbol.EnumLabelsAction = parser.popSymbol.asInstanceOf[Symbol.EnumLabelsAction]
    if (e < 0 || e >= top.size) {
      throw new AvroTypeException("Enumeration out of range: max is " + top.size + " but received " + e)
    }
    out.writeString(top.getLabel(e))
  }

  @throws[IOException]
  override def writeArrayStart(): Unit = {
    parser.advance(Symbol.ARRAY_START)
    out.writeStartArray()
    push()
    isEmpty.set(depth)
  }

  @throws[IOException]
  override def writeArrayEnd(): Unit = {
    if (!(isEmpty.get(pos))) {
      parser.advance(Symbol.ITEM_END)
    }
    pop()
    parser.advance(Symbol.ARRAY_END)
    out.writeEndArray()
  }

  @throws[IOException]
  override def writeMapStart(): Unit = {
    push()
    isEmpty.set(depth)
    parser.advance(Symbol.MAP_START)
    out.writeStartObject()
  }

  @throws[IOException]
  override def writeMapEnd(): Unit = {
    if (!(isEmpty.get(pos))) {
      parser.advance(Symbol.ITEM_END)
    }
    pop()
    parser.advance(Symbol.MAP_END)
    out.writeEndObject()
  }

  @throws[IOException]
  override def startItem(): Unit = {
    if (!(isEmpty.get(pos))) {
      parser.advance(Symbol.ITEM_END)
    }
    super.startItem()
    isEmpty.clear(depth)
  }

  @throws[IOException]
  override def writeIndex(unionIndex: Int): Unit = {
    parser.advance(Symbol.UNION)
    val top: Symbol.Alternative = parser.popSymbol.asInstanceOf[Symbol.Alternative]
    val symbol: Symbol = top.getSymbol(unionIndex)
    if (symbol ne Symbol.NULL) {
      //union that represents an option is a union of [null, X] - in that case we don't write the label
      if ((top.size() != 2) || (top.getSymbol(0) ne Symbol.NULL)) {
        out.writeStartObject()
        out.writeFieldName(top.getLabel(unionIndex))
        parser.pushSymbol(Symbol.UNION_END)
      }
    }
    parser.pushSymbol(symbol)
  }

  @throws[IOException]
  override def doAction(input: Symbol, top: Symbol): Symbol = {
    if (top.isInstanceOf[Symbol.FieldAdjustAction]) {
      val fa: Symbol.FieldAdjustAction = top.asInstanceOf[Symbol.FieldAdjustAction]
      out.writeFieldName(fa.fname)
    } else if (top eq Symbol.RECORD_START) {
      out.writeStartObject()
    } else if ((top eq Symbol.RECORD_END) || (top eq Symbol.UNION_END)) {
      out.writeEndObject()
    } else if (top ne Symbol.FIELD_END) {
      throw new AvroTypeException("Unknown action symbol " + top)
    }
    return null
  }
}

