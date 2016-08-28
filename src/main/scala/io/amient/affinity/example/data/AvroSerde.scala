package io.amient.affinity.example.data
import org.apache.avro.Schema

class AvroSerde extends io.amient.affinity.core.data.AvroSerde {
  override def register: Seq[(Class[_], Schema)] = Seq (
    classOf[Vertex] -> Vertex.schema,
    classOf[Edge] -> Edge.schema,
    classOf[Component] -> Component.schema
  )
}
