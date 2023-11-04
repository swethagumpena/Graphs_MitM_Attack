package utils

import NetGraphAlgebraDefs.NodeObject
import com.typesafe.config.{Config, ConfigFactory}

object FindMatchingElement {
  // Calculates the Jaccard Similarity between two NodeObjects
  def jaccardSimilarity(perturbedNodeObject: NodeObject, originalNodeObject: NodeObject): Double = {

    // Convert NodeObjects to Sets for easier set operations
    val set1 = originalNodeObject.toSet
    val set2 = perturbedNodeObject.toSet
    // Calculate the size of the intersection between set1 and set2
    val intersectionSize = set1.intersect(set2).size
    // Calculate the size of the union of set1 and set2
    val unionSize = set1.union(set2).size
    // Calculate the Jaccard Similarity
    if (unionSize == 0) 0.0 else intersectionSize.toDouble / unionSize.toDouble
  }

  // Finds the matching element with the highest similarity score
  def matchedElement(perturbedNode: NodeObject, originalNodes: Array[NodeObject]): Option[(Int, Int, Double)] = {
    val config: Config = ConfigFactory.load("application.conf")
    val matchingThreshold = config.getDouble("mitmAttack.similarityThreshold")

    // Calculate similarity scores for all original nodes and filter them based on the threshold
    val matchingElementArr = originalNodes.flatMap { originalNode =>
      val score = jaccardSimilarity(perturbedNode, originalNode)
      if (score > matchingThreshold) Some((originalNode.id, perturbedNode.id, score))
      else None
    }

    // If there are matching elements, return the one with the highest similarity score
    if (matchingElementArr.nonEmpty) {
      val maxScoreElement = matchingElementArr.maxBy(_._3)
      Some(maxScoreElement)
    } else {
      // In case of no matching element found
      None
    }
  }
}