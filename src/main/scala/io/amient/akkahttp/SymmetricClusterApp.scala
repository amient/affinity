package io.amient.akkahttp

/**
  * Created by mharis on 17/08/2016.
  */
object SymmetricClusterApp extends App {

  SymmetricHttpNode.main(Seq("2551","localhost","8081","4","1,3").toArray)
  SymmetricHttpNode.main(Seq("2552","localhost","8082","4","2,4").toArray)

}
