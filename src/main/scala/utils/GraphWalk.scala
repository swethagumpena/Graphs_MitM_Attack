package utils

import NetGraphAlgebraDefs.{Action, NodeObject}
import org.apache.spark.graphx.{Graph, VertexId}
import org.apache.spark.util.CollectionAccumulator
import scala.annotation.tailrec
import scala.util.Random
import org.slf4j.LoggerFactory

object GraphWalk {
  private val logger = LoggerFactory.getLogger(getClass)
  //   Define the randomWalk function with an accumulator for visited nodes
  def randomWalk(graph: Graph[NodeObject, Action], startNode: VertexId, maxSteps: Int, visitedNodesAcc: CollectionAccumulator[VertexId]): List[VertexId] = {
    @tailrec
    def randomWalkRecursive(currentNode: VertexId, steps: Int, path: List[VertexId]): List[VertexId] = {
      if (steps >= maxSteps - 1 || visitedNodesAcc.value.contains(startNode)) {
        path.reverse
      } else {
        val neighbors = graph.edges.filter(_.srcId == currentNode).map(_.dstId).collect()
        if (neighbors.isEmpty) path.reverse
        else {
          val unvisitedNeighbors = neighbors.filter(!visitedNodesAcc.value.contains(_))
          if (unvisitedNeighbors.isEmpty) path.reverse
          else {
            val nextNode = unvisitedNeighbors(Random.nextInt(unvisitedNeighbors.length))
            randomWalkRecursive(nextNode, steps + 1, nextNode :: path)
          }
        }
      }
    }
    logger.info(s"Starting randomWalk with startNode: $startNode, maxSteps: $maxSteps")
    val result = randomWalkRecursive(startNode, 0, List(startNode))
    logger.info(s"RandomWalk result: $result")
    result
  }
}
