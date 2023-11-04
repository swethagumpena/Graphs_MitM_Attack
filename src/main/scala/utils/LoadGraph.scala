package utils

import NetGraphAlgebraDefs.{Action, NetGraphComponent, NodeObject}
import org.slf4j.LoggerFactory

import java.io.{FileInputStream, InputStream, ObjectInputStream}
import java.net.URL
import scala.util.Try

object LoadGraph {
  private val logger = LoggerFactory.getLogger(getClass)

  def load(filePath: String): (List[NodeObject], List[Action]) = {
    val inputStream: Option[InputStream] = if (filePath.startsWith("http://") || filePath.startsWith("https://")) {
      Try(new URL(filePath).openStream()).toOption
    } else {
      Try(new FileInputStream(filePath)).toOption
    }
    inputStream match {
      case Some(stream) =>
        val objectInputStream = new ObjectInputStream(stream)
        val ng = objectInputStream.readObject().asInstanceOf[List[NetGraphComponent]]
        val nodes = ng.collect { case node: NodeObject => node }
        val edges = ng.collect { case edge: Action => edge }
        objectInputStream.close() // Close the stream after use
        (nodes, edges)
      case None =>
        logger.error("Invalid file path or URL")
        throw new IllegalArgumentException("Invalid file path or URL")
    }
  }
}
