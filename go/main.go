package main

import (
	"flag"
	avrolib "github.com/amient/avro"
	"github.com/amient/goconnect"
	"github.com/amient/goconnect/coder"
	"github.com/amient/goconnect/coder/avro"
	"github.com/amient/goconnect/io/kafka1"
)

var (
	sourceKafkaBootstrap    = flag.String("source-kafka-bootstrap", "localhost:9092", "Kafka Bootstrap servers for the source topis")
	sourceKafkaGroup        = flag.String("source-kafka-group", "goc-avro-poc", "Source Kafka Consumer Group")
	sourceKafkaTopic        = flag.String("source-kafka-topic", "avro", "Source Kafka Topic")
	soureKafkaUsername      = flag.String("source-username", "", "Source Kafka Principal")
	soureKafkaPassword      = flag.String("source-password", "", "Source Kafka Password")
	sourceSchemaRegistryUrl = flag.String("source-schema-registry-url", "http://localhost:8081", "Source Kafka Topic")
	//
	sinkKafkaBootstrap    = flag.String("sink-kafka-bootstrap", "localhost:9092", "Kafka Destination Bootstrap servers")
	sinkKafkaTopic        = flag.String("sink-kafka-topic", "avro-copy", "Destination Kafka Topic")
	sinkKafkaUsername     = flag.String("sink-username", "", "Source Kafka Principal")
	sinkKafkaPassword     = flag.String("sink-password", "", "Source Kafka Password")
	sinkSchemaRegistryUrl = flag.String("sink-schema-registry-url", "http://localhost:8081", "Source Kafka Topic")
	targetSchema, err = avrolib.ParseSchema(`{
  "type": "record",
  "name": "Example",
  "fields": [
    {
      "name": "seqNo",
      "type": "long",
      "default": 0
    },
    {
      "name": "timestamp",
      "type": "long",
      "default": -1
    }
  ]}`)
)

func main() {

	flag.Parse()

	pipeline := goconnect.NewPipeline().WithCoders(coder.Registry())

	pipeline.
		Root(&kafka1.Source{
			Topic: *sourceKafkaTopic,
			ConsumerConfig: kafka1.ConfigMap{
				"bootstrap.servers": *sourceKafkaBootstrap,
				"group.id":          *sourceKafkaGroup,
				"sasl.username":     *soureKafkaUsername,
				"sasl.password":     *soureKafkaPassword,
				"auto.offset.reset": "earliest"}}).Buffer(10000).

		Apply(&avro.SchemaRegistryDecoder{Url: *sourceSchemaRegistryUrl}).

		Apply(&avro.GenericProjector{targetSchema }).

		Apply(new(avro.GenericEncoder)).

		Apply(&avro.SchemaRegistryEncoder{Url: *sinkSchemaRegistryUrl, Subject: *sinkKafkaTopic + "-value"}).

		Apply(&kafka1.Sink{
			Topic: *sinkKafkaTopic,
			ProducerConfig: kafka1.ConfigMap{
				"bootstrap.servers": *sinkKafkaBootstrap,
				"sasl.username":     *sinkKafkaUsername,
				"sasl.password":     *sinkKafkaPassword,
				"linger.ms":         100}})

	pipeline.Run()

}