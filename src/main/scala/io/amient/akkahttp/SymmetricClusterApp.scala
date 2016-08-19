package io.amient.akkahttp

object SymmetricClusterApp extends App {

  SymmetricNode.main(Seq("2551","127.0.0.1","8081","4","1,3").toArray)
  SymmetricNode.main(Seq("2552","127.0.0.1","8082","4","2,4").toArray)

}
