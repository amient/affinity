package io.amient.affinity.core.actor

import akka.actor.Actor
import akka.event.Logging
import io.amient.affinity.Conf
import io.amient.affinity.core.state.KVStoreGlobal
import io.amient.affinity.core.storage.State

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.control.NonFatal

trait GlobalState extends Actor {

  private val log = Logging.getLogger(context.system, this)
  private val conf = Conf(context.system.settings.config)
  private val declaredGlobals = mutable.Map[String, State[_, _]]()
  private val declaredGlobals2 = mutable.Map[String, KVStoreGlobal[_, _]]()
  private lazy val globals = declaredGlobals.toMap
  private lazy val globals2 = declaredGlobals2.toMap
  private var started = false

  final def global[K: ClassTag, V: ClassTag](globalStateStore: String): State[K, V] = {
    if (started) throw new IllegalStateException("Cannot declare state after the actor has started")
    declaredGlobals.get(globalStateStore) match {
      case Some(globalState) => globalState.asInstanceOf[State[K, V]]
      case None =>
        val bc = State.create[K, V](globalStateStore, 0, conf.Affi.Global(globalStateStore), 1, context.system)
        declaredGlobals += (globalStateStore -> bc)
        bc
    }
  }

  final def global2[K: ClassTag, V: ClassTag](globalStateStore: String): KVStoreGlobal[K,V] = {
    if (started) throw new IllegalStateException("Cannot declare state after the actor has started")
    declaredGlobals2.get(globalStateStore) match {
      case Some(globalState) => globalState.asInstanceOf[KVStoreGlobal[K, V]]
      case None =>
        val bc = new KVStoreGlobal[K, V](globalStateStore, conf.Affi.Global(globalStateStore), context)
        declaredGlobals2 += (globalStateStore -> bc)
        bc
    }
  }

  abstract override def preStart(): Unit = {
    super.preStart()
    started = true
    // first bootstrap all global state stores
    globals.values.foreach(_.boot)
    globals2.values.foreach(_.boot)
    // any thereafter switch them to passive mode for the remainder of the runtime
    globals.values.foreach(_.tail)
    globals2.values.foreach(_.tail)
  }

  abstract override def postStop(): Unit = try {
    log.debug("Closing global state stores")
    globals.foreach { case (identifier, state) => try {
        state.close()
      } catch {
        case NonFatal(e) => log.error(e, s"Could not close cleanly global state: $identifier ")
      }
    }
    globals2.foreach { case (identifier, state) => try {
      state.close()
    } catch {
      case NonFatal(e) => log.error(e, s"Could not close cleanly global state: $identifier ")
    }
    }
  } finally {
    super.postStop()
  }


}
