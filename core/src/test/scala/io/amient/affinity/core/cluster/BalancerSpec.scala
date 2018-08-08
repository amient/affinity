package io.amient.affinity.core.cluster

import org.scalacheck.Gen
import org.scalatest.prop.PropertyChecks
import org.scalatest.{Matchers, PropSpec}

class BalancerSpec extends PropSpec with PropertyChecks with Matchers {

  def partitions: Gen[Int] = Gen.choose(1, 1000)

  def nodes: Gen[Int] = Gen.choose(1, 100)

  def replFactors: Gen[Int] = Gen.choose(1, 100)

  property("all partitions should be represented and replicated exactly when num.nodes >= repl.factor") {
    forAll(partitions, replFactors, nodes) { (numPartitions: Int, replFactor: Int, numNodes: Int) =>
      val assignment: IndexedSeq[Vector[Int]] = Balancer.generateAssignment(numPartitions, replFactor, numNodes)
      assignment.size should be (numNodes)
      assignment.map(_.toSet).reduce(_ ++ _) should equal ((0 until numPartitions).toSet)
      whenever (numNodes >= replFactor) {
        val replicas = assignment.flatMap(_.map(pi => pi -> 1)).groupBy(_._1).mapValues(_.map(_._2).sum)
        replicas.foreach { case (_, r) => r should be (replFactor) }
      }
    }
  }

  property("generate balanced and deterministic assignment for uneven partitioning scheme") {
    Balancer.generateAssignment(7, 6, 9) should be(
      Vector(
        Vector(0,1,3,5),
        Vector(0,1,2,4,6),
        Vector(0,1,2,3,5),
        Vector(1,2,3,4,6),
        Vector(0,2,3,4,5),
        Vector(1,3,4,5,6),
        Vector(0,2,4,5,6),
        Vector(1,3,5,6),
        Vector(0,2,4,6)
      )
    )
  }

  property("generate balanced and deterministic assignment for even partitioning scheme") {
    Balancer.generateAssignment(6, 3, 6) should be(
      Vector(
        Vector(0,2,4),
        Vector(1,3,5),
        Vector(0,2,4),
        Vector(1,3,5),
        Vector(0,2,4),
        Vector(1,3,5)
      )
    )
  }
}
