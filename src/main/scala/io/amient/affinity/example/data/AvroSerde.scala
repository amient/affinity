package io.amient.affinity.example.data

class AvroSerde extends io.amient.affinity.core.data.AvroSerde {

  register(classOf[Vertex], Vertex.schema)
  register(classOf[Edge], Edge.schema)
  register(classOf[Component], Component.schema)

}
