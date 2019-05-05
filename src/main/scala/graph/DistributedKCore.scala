package graph

import org.apache.spark.graphx.{EdgeTriplet, VertexId}


import org.apache.spark.graphx._
import scala.collection.Map
import scala.graph.KCoreVertex


/**
  * Oggetto che contiene la computazione effettiva dell'algoritmo k-core decomposition
  * L'implementazione e' supportata da GraphX che implementa il paradigma di scambio
  * dei messaggi Pregel. Il protocollo si basa su dei superturni nei quali ogni nodo:
  * -elabora la sua coda di messaggi
  * -spedisce messaggi ad altri nodi di cui conosce l'identificatore (in questo caso solo i vicini)
  * Pregel si occupa della gestione della comunicazione. A ogni superturno, fornisce ai nodi
  * un oggetto che contiene la coda dei messaggi. E' pertanto cura del programmatore,
  * fare in modo che tali messaggi siano componibili e consegnabili come un'unica entita'.
  * La rappresentazione del grafo in Pregel e' basata su archi orientati, serve dunque tenere
  * anche conto della bidirezionalita' del grafo.
  */
object DistributedKCore {
  val dummyMessage: String = Long.MinValue + "->-1"

  def toTuple(str: String) = {
    val splitted = str.split("->")
    (splitted(0).toLong.asInstanceOf[VertexId], splitted(1).toInt)
  }

  /**
    * Definisce il comportamento di un nodo in ogni superciclo.
    * Nello specifico a ogni turno, il nodo esamina la propria coda di messaggi, e se
    * trova dei cambiamenti rispetto alle sue stime ricalcola la propria coreness. In
    * caso di cambiamento di questa, invia la nuova coreness ai propri vicini.
    * @param id il numero identificatore del nodo
    * @param attr l'istanza del nodo
    * @param msg la coda dei messaggi
    * @return l'oggetto del nodo, aggiornato
    */
  def vertexProgram(id: VertexId, attr: KCoreVertex, stringMsg: String) = {
    val nVertex = new KCoreVertex(id)
    nVertex.updated = false
    nVertex.coreness = attr.coreness
    nVertex.receivedMsg = attr.receivedMsg
    nVertex.est = attr.est
    nVertex.iterationToConverge = attr.iterationToConverge

    val msg = stringMsg.split(';').toSet
    msg.map(toTuple).filter(tuple => !nVertex.est.contains(tuple._1)).foreach(tuple => {
      // Se la nuova stima è più bassa di quella presente
      nVertex.est = nVertex.est + (tuple._1 -> Int.MaxValue)
    })
    if (stringMsg != dummyMessage && msg.nonEmpty) {
      nVertex.incReceived(msg.size)
      msg.map(toTuple).foreach(tuple => {
        // Se la nuova stima è più bassa di quella presente
        if (tuple._2 < nVertex.est.getOrElse(tuple._1, nVertex.est(tuple._1))) {
          nVertex.est = nVertex.est + (tuple._1 -> tuple._2)
          val computedCoreness = nVertex.computeIndex()
          if (computedCoreness < nVertex.coreness) {
            nVertex.coreness = computedCoreness
            nVertex.updated = true
          }
        }
      })
    } else {
      nVertex.updated = true
    }
    if (nVertex.updated) {
      nVertex.iterationToConverge = nVertex.iterationToConverge + 1
      // nVertex.est.foreach(println)
    }
    nVertex
  }

  /**
    * Crea e invia a un nodo una struttura contenente la coda dei messaggi di un superciclo
    * @param triplet la rappresentazione a triple dei destinatari e dei loro messaggi
    * @return un iteratore sulla struttura contenente i messaggi
    */
  def sendMessage(triplet: EdgeTriplet[KCoreVertex, String]) = {
    if (triplet.srcAttr.updated || triplet.attr == dummyMessage) {
      Iterator((triplet.dstId, triplet.srcAttr.nodeId + "->" + triplet.srcAttr.coreness))
    } else {
      Iterator.empty
    }
  }

  /**
    * Unisce due insiemi di messaggi in coppie (nodo, coreness)
    * @param msg1 primo insieme di messaggi
    * @param msg2 secondo insieme di messaggi
    * @return dizionario contenente tutti i messaggi
    */
  def mergeMessages(msg1: String, msg2: String) = {
    msg1 + ";" + msg2
  }

  /**
    * Esegue l'algoritmo vero e proprio sull'intera rete.
    * @param graph il grafo
    * @param maxIterations il numero di supercicli da eseguire (si consiglia >=40)
    * @return il grafo a computazione eseguita
    */
  def decomposeGraph(graph: Graph[KCoreVertex, String], maxIterations: Int) = {
    graph.pregel(dummyMessage, maxIterations = maxIterations, EdgeDirection.In)(
      (id, attr, msg) => vertexProgram(id, attr, msg),
      triplet => sendMessage(triplet),
      (coreness1, coreness2) => mergeMessages(coreness1, coreness2)
    )
  }

}
