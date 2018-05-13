# Affinity Avro Module

This module can be used in any application that requires mapping between
avro records registered in a central schema registry and scala case classes.

It can be used on its own for runtime serialization, but there are other
modules that wrap around it and provide higher-level implementations for
Akka, Kafka, Kafka Streams, Spark serializers/deserializers and more.

## Usage

SBT

    libraryDependencies += "io.amient.affinity" %% "avro-scala" % "0.4.0"

Gradle

    compile group: 'io.amient.affinity', name: 'avro-scala_2.11', version: '0.4.0'

Maven

    <dependency>
        <groupId>io.amient.affinity</groupId>
        <artifact>0.4.0</version>
    </dependency>

It is a very light-weight module and besides the scala libarary it only carries the following dependencies:

    org.apache.avro:avro
    com.typesafe:config
    com.101tec:zkclient

The ZooKeeper client is there because there is an implementation of
schema registry based on ZooKeeper for simpler deployments but it may
be moved in future to a separate module avro-zookeeper-registry.

## Supported types

Below is the table of all supported types, all which support default values.

    Scala Type                      Avro Type            Note
    ----------------------------------------------------------------------------------------------------
    Null                            null
    Boolean                         boolean
    Int                             int                  avro compressed binary
    @Fixed Int                      fixed(4)             4-byte big endian 
    Long                            long                 avro compressed binary
    @Fixed Long                     fixed(8)             8-byte big endian
    Float                           float
    Double                          double
    String                          string               UTF-8
    @Fixed(size) String             fixed(size)          ASCII
    Array[Byte]                     bytes                varible length byte array
    @Fixed Array[Byte]              fixed(size)          fixed size byte array
    UUID                            fixed(16)            16-byte uuid representation, same as when @Fixed 
    Map[String, T]   †              map(T)               Maps have to have String keys
    Iterable[T]      †              array(T)             List[T], Seq[T], Set[T], etc.
    Enumeration      †              enum                 only scala enums are suppored
    Option[T]        †              union(null,T)        
    case class                      record               nested schemas are allowed
    ----------------------------------------------------------------------------------------------------    
    † Generic types cannot be top-level schemas, they can be nested in a case class record - this is because
      generics don't have concrete static type so have to be evaluated at runtime and cannot be efficiently cached. 
      There would be a significant pefromance penalty for that although conceptually it could be possible.

### Note on Top-level primitive types

Each of the types above can be also used as a top-level type in your application but not all the
types are always meaningful as such.


## Example

Any top-level case class that extends abstract class AvroRecord becomes automatically an avro SpecificRecord.

    package com.example.domain

    object Gender extends Enumeration {
      type Gender = Value
      val Male, Female = Value
    }

    case class Person(  name: String,
                        gender: Gender.Value,
                        score: Option[Double] = None) extends AvroRecord


AvroRecord global object provides some useful factory methods

    val schema1 = AvroRecord.inferSchema[Person]
    val schema2 = AvroRecord.inferSchema(classOf[Person])
    val schema3 = AvroRecord.inferSchema("com.example.domain.Person")

All of the schemas are equivalent and can be used in various scenarios
but an instance of this class also has all the methods of a Specific record
, including getSchema:

    val person: Person = Person("Jeff Lebowski", Gender.Male, Some(11.5))
    val schema: Schema = person.getSchema

This schema is the same as the 3 schemas above which are derived from the type directly
and looks like this:

    println(schema)

    {
       "type":"record",
       "name":"Person",
       "namespace":"com.example.domain",
       "fields":[
          {
             "name":"name",
             "type":"string"
          },
          {
             "name":"gender",
             "type":{
                "type":"enum",
                "name":"Gender",
                "symbols":[
                   "Male",
                   "Female"
                ]
             }
          },
          {
             "name":"score",
             "type":[
                "null",
                "double"
             ],
             "default":null
          }
       ]
    }

The above class can be converted to binary bytes with the standard Apache Avro tools:

    val output = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(output, null)
    val writer = new GenericDatumWriter[Any](person.schema)
    writer.write(person, encoder)
    encoder.flush()
    val bytes: Array[Byte] = output.toByteArray

which is the same thing using the AvroRecord compation object method:

    val output = new ByteArrayOutputStream()
    AvroRecord.write(person, person.getSchema, output)
    val bytes: Array[Byte] = output.toByteArray

or if you want byte array directly:

    val bytes: Array[Byte] = AvroRecord.write(person, person.getSchema)

converting bytes back to case class requires besides the schema also a type tag

    val readerSchema = AvroRecord.inferSchema[Person]
    val person: Person = AvroRecord.read[Person](bytes, readerSchema)

## Schema Projection

AvroRecord factory methods have variants which take 2 schemas: writer schema and read schema.
The reader schema is the one that matches the current runtime case class.
The writer schema is the schema which was used to serialize an instance into a binary.
If we had serialized the person instance with a case class that didn't have the score: Option[Double]
field it would be possible to project it onto our current runtime defintion of case class Person
because our current defintion has a default value None for the field:

    val readerSchema = AvroRecord.inferSchema[Person]
    val person: Person = AvroRecord.read[Person](bytes, writerScheam, readerSchema)

More information about schema compatibility and projection can be found on Apache Avro website.

## Schema Registries

AvroRecord class and its companion object described above is only the bottom layer of the
serialization stack.

...

### Usage with provided Kafka serde tools

All the functionality provided by the AvroRecord, AvroSerde and AvroSchemaRegistry
is packaged in [avro-serde-kafka](../kafka/avro-serde-kafka) module which provides implementations 
of the following standard kafka interfaces:

    io.amient.affinity.kafka.KafkaAvroSerializer implements org.apache.kafka.common.serialization.Serializer
    io.amient.affinity.kafka.KafkaAvroDeserializer implements org.apache.kafka.common.serialization.Deserializer
    io.amient.affinity.kafka.KafkaAvroSerde[T] implements org.apache.kafka.common.serialization.Serde[T]

The only extra dependency that these modules carry is the respective version *org.apache.kafka:kafka-clients*.
There is also a separate module that provides class for the standard console consumer:

    io.amient.affinity.kafka.AvroMessageFormatter implements kafka.common.MessageFormatter

See [avro-formatter-kafka](../kafka/avro-formatter-kafka) for usage and options of the formatter.


**NOTE: the above serde tools can be used interchangably with serializers and deserializers that
ship with confluent schema registry - the wire format is the same, except with affinity tools
you can work with type-safe case classes instead of generic avro records.**


## AvroJsonConverter

    AvroJsonConverter.toJson(avro: Any): String
    AvroJsonConverter.toJson(writer: Writer, avro: Any): Unit
    AvroJsonConverter.toAvro(json: String, schema: Schema): Any

## Best Practices

- the package coordinates of the case class should remain the same for the life of the definition
    - this also applies to the actual name of the case class
    - this is because there are no aliases for the top-level defintions, only for fields
    - aliasing the case classes may be added later
- try to avoid null as a default value if the field has a natrual or empty representation
- if you think null is a natural default value, then use Option
- try to use primitives where possible
- if an exsting field needs to be renamed and at the same time should inherit the values the @Alias needs to be applied
    - e.g. case class Example(@Alias(<old-field1>[, <old-field2>, [...]]) <new-field> <TYPE>
- it's possbile and perfectly ok to use camel case or other cases containing capital leters
    - however, there an issue with the existing connect hive stack where the field names are lowercased
        in some situations and not in others which leads to "unkown field" field exceptiosn thrown by
        hive partitioner code
    - even though the above is a bug in the kafka connect hdfs connector, possibly hive internals there
        may be other backends that may have issues with cases and since avro is meant to be cross-platform
        serialization, using camel case needs some consideration

