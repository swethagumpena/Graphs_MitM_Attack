package utils

import NetGraphAlgebraDefs.NodeObject
import com.typesafe.config.{Config, ConfigFactory}

object FindMatchingElement {
  def jaccardSimilarity(perturbedNodeObject: NodeObject, originalNodeObject: NodeObject): Double = {

    val set1 = originalNodeObject.toSet
    val set2 = perturbedNodeObject.toSet
    // Calculate the size of the intersection between set1 and set2
    val intersectionSize = set1.intersect(set2).size
    // Calculate the size of the union of set1 and set2
    val unionSize = set1.union(set2).size
    // Calculate the Jaccard Similarity
    if (unionSize == 0) 0.0 else intersectionSize.toDouble / unionSize.toDouble
  }

  def calculateScore(perturbedNode: NodeObject, originalNodes: Array[NodeObject]): Option[(Int, Int, Double)] = {
    val config: Config = ConfigFactory.load("application.conf")
    val matchingThreshold = config.getDouble("mitmAttack.similarityThreshold")

    val matchingElementArr = originalNodes.flatMap { originalNode =>
      val score = jaccardSimilarity(perturbedNode, originalNode)
      if (score > matchingThreshold) Some((originalNode.id, perturbedNode.id, score))
      else None
    }

    if (matchingElementArr.nonEmpty) {
      val maxScoreElement = matchingElementArr.maxBy(_._3)
      Some(maxScoreElement)
    } else {
      None
    }
  }
}