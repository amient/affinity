package io.amient.affinity.avro

import io.amient.affinity.avro.record.AvroRecord
import org.apache.avro.Schema
import org.scalatest.{FlatSpec, Matchers}

object Status extends Enumeration {
  type Status = Value
  val OK, FAILED = Value
}

case class Referenced(
       A: Status.Value, B: Status.Value, C: Map[String, Status.Value], D: List[Status.Value], E: Option[Status.Value]) extends AvroRecord

class AvroSchemaSpec extends FlatSpec with Matchers {

  "AvroRecord" should "not fail when referencing the same type in a single schema" in {
    val schemaJson = AvroRecord.inferSchema[Referenced].toString(false)
    println(schemaJson)
    new Schema.Parser().parse(schemaJson)
  }

  "AvroRecord" should "1" in {
    new Schema.Parser().parse(
      """{"type":"record","namespace":"com.trustelevate.vpc.domain","name":"Parent","fields":[{"name":"pid","type":"long"},{"name":"registered","default":false,"type":"boolean"},{"name":"consents","default":{},"type":{"type":"map","values":{"type":"record","name":"Consent","fields":[{"name":"username","type":"string"},{"name":"contact","type":{"type":"record","name":"CredentialKey","fields":[{"name":"kind","type":{"type":"enum","name":"CredentialType","symbols":["FIRST_NAME","LAST_NAME","EMAIL","DOB","ADDRESS","PARENT","PHONE"]}},{"name":"value","type":"string"}]}},{"name":"service","type":"string"},{"name":"consentAge","default":0,"type":"int"},{"name":"status","default":"PENDING","type":{"type":"enum","name":"ConsentStatus","symbols":["NOT_REQUIRED","PENDING","APPROVED","REJECTED"]}},{"name":"requestedUTC","type":"long"},{"name":"updatedUTC","default":-1,"type":"long"},{"name":"child","default":0,"type":"long"},{"name":"verification","default":"UNKNOWN","type":{"type":"enum","name":"VerificationStatus","symbols":["UNKNOWN","CONFIRMED","VERIFIED","FAILED"]}},{"name":"requestToken","default":"","type":"string"}]}}},{"name":"children","default":{},"type":{"type":"map","values":{"type":"record","name":"Child","fields":[{"name":"pii","type":{"type":"array","items":"CredentialKey"}},{"name":"verification","type":"VerificationStatus"},{"name":"verificationTimestamp","default":-1,"type":"long"}]}}},{"name":"password","default":null,"type":["null","string"]},{"name":"action","default":"NONE","type":{"type":"enum","name":"UserAction","symbols":["CREATE_PASSWORD","RESET_PASSWORD","NONE"]}}]}"""
    )
  }

}
