package io.amient.affinity.core.cluster

object Balancer {

  /**
    *
    * @param numPartitions numPartitions to balance
    * @param replFactor number of required replicas for each partitions
    * @param numNodes number of nodes across which to balance the replicas
    * @return a indexed sequence of nodes and their partition assignment vectors
    */
  def generateAssignment(numPartitions: Int, replFactor: Int, numNodes: Int): IndexedSeq[Vector[Int]] = {
    val vectors = for (_ <- 0 until numNodes) yield Vector.newBuilder[Int]
    val R = math.round(numNodes.toFloat / replFactor)
    for (p <- 0 until numPartitions) {
      for (r <- 0 until replFactor) {
        val n = (p + r * R) % numNodes
        vectors(n) += p
      }
    }
    vectors.map(_.result().sorted)
  }

}
