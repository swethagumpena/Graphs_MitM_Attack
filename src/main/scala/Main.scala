import NetGraphAlgebraDefs.{Action, NodeObject}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.slf4j.LoggerFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.CollectionAccumulator
import utils.LoadGraph
import utils.FindMatchingElement.calculateScore
import utils.ParseYAML.parseFile
import utils.WriteResults.writeContentToFile

import scala.util.Random

object Main {
  private val logger = LoggerFactory.getLogger(getClass)

  // Parse a NodeObject from its string representation
  def parseNodeObject(nodeString: String): NodeObject = {
    val pattern = """NodeObject\((\d+),(\d+),(\d+),(\d+),(\d+),(\d+),(\d+),(\d+),([\d.]+(?:[Ee][+-]?\d+)?),(\w+)\)""".r
    nodeString match {
      case pattern(id, children, props, currentDepth, propValueRange, maxDepth,
      maxBranchingFactor, maxProperties, storedValue, valuableData) =>
        NodeObject(
          id.toInt,
          children.toInt,
          props.toInt,
          currentDepth.toInt,
          propValueRange.toInt,
          maxDepth.toInt,
          maxBranchingFactor.toInt,
          maxProperties.toInt,
          storedValue.toDouble,
          valuableData.toBoolean
        )
      case _ =>
        logger.trace("Invalid NodeObject string format")
        throw new IllegalArgumentException("Invalid NodeObject string format" + nodeString)
    }
  }

  //   Define the randomWalk function with an accumulator for visited nodes
  def randomWalk(graph: Graph[NodeObject, Action], startNode: VertexId, maxSteps: Int, visitedNodesAcc: CollectionAccumulator[VertexId]): List[VertexId] = {
    var currentNode = startNode
    var steps = 0
    var path = List(currentNode)

    while (steps < maxSteps && !visitedNodesAcc.value.contains(startNode)) {
      val neighbors = graph.edges.filter(_.srcId == currentNode).map(_.dstId).collect()
      if (neighbors.isEmpty) return path.reverse

      // Filter out nodes that have been visited
      val unvisitedNeighbors = neighbors.filter(!visitedNodesAcc.value.contains(_))
      if (unvisitedNeighbors.isEmpty) return path.reverse
      // Choose a random node from unvisited neighbors
      currentNode = unvisitedNeighbors(Random.nextInt(unvisitedNeighbors.length))
      path = currentNode :: path
      steps += 1
    }
    path.reverse
  }

  def main(args: Array[String]): Unit = {
    logger.info("In spark application")

    val isRunningOnAWS = sys.env.contains("AWS_REGION") || sys.env.contains("AWS_EXECUTION_ENV")

    val sc = if (isRunningOnAWS) {
      val spark = SparkSession.builder().appName("Graphs_MitM_Attack").getOrCreate()
      spark.sparkContext
    } else {
      val conf = new SparkConf().setAppName("RandomWalksApp").setMaster("local[4]") // Set master to local with 4 cores
      new SparkContext(conf)
    }

    val (originalNodes, originalEdges) = LoadGraph.load(args(0))
    val (perturbedNodes, perturbedEdges) = LoadGraph.load(args(1))

    if (originalNodes.isEmpty || originalEdges.isEmpty || perturbedNodes.isEmpty || perturbedEdges.isEmpty) {
      logger.warn("Input is not of the right format")
    } else logger.info("Graphs successfully loaded")

    val originalNodesArray = originalNodes.toArray.map(_.toString)
    val parsedOriginalNodes: Array[NodeObject] = originalNodesArray.map(parseNodeObject)

    val originalNodeIDsWithValuableData: Array[Int] = parsedOriginalNodes.collect {
      case node if node.valuableData => node.id
    }

    val perturbedNodesArray = perturbedNodes.toArray.map(_.toString)
    val parsedPerturbedNodes: Array[NodeObject] = perturbedNodesArray.map(parseNodeObject)

    // Create an RDD for the nodes
    val nodes: RDD[(VertexId, NodeObject)] = sc.parallelize(parsedPerturbedNodes).map { node =>
      (node.id, node)
    }

    val edges: RDD[Edge[Action]] = sc.parallelize(perturbedEdges.toArray.map(action => Edge(action.fromNode.id, action.toNode.id, action)))

    val graph = Graph(nodes, edges)

    val sAttacks = scala.collection.mutable.Set[Int]()
    val fAttacks = scala.collection.mutable.Set[Int]()

    val yamlData = parseFile(args(2))
    val addedNodesList = yamlData("AddedNodes").map(_.toInt)
    val modifiedNodesList = yamlData("ModifiedNodes").map(_.toInt)

    val maxSteps = 10 // Set the maximum number of steps for the random walk

    // Define an accumulator for visited nodes
    val visitedNodesAcc = sc.collectionAccumulator[VertexId]("VisitedNodes")

    // Function to perform random walks from randomly selected nodes
    def performRandomWalks(graph: Graph[NodeObject, Action], maxSteps: Int, visitedNodesAcc: CollectionAccumulator[VertexId]): Unit = {
      var iteration = 0

      while (visitedNodesAcc.value.toArray.toSet.size < parsedPerturbedNodes.length * 0.9) {
        val startNode = graph.vertices.map(_._1).collect()(Random.nextInt(graph.vertices.count().toInt)) // Choose a random starting node
        val walk = randomWalk(graph, startNode, maxSteps, visitedNodesAcc)

        // Process the walk and update successful/failure attacks
        walk.foreach { vertexId =>
          val node = nodes.lookup(vertexId).headOption.orNull // Lookup the node by vertexId
          val walkScoreTuple = calculateScore(node, parsedOriginalNodes)
          walkScoreTuple.foreach { walkScoreTuple =>
            // added - check perturbed node ID, modified - check original node ID
            if (originalNodeIDsWithValuableData.contains(walkScoreTuple._1)) {
              if (addedNodesList.contains(walkScoreTuple._2) || modifiedNodesList.contains(walkScoreTuple._1)) {
                fAttacks += walkScoreTuple._2
              } else {
                sAttacks += walkScoreTuple._2
              }
            }
          }
          visitedNodesAcc.add(vertexId)
        }
        iteration += 1
      }
    }

    // Perform random walks
    performRandomWalks(graph, maxSteps, visitedNodesAcc)

    val content = s"SuccessfulAttacks: ${sAttacks.mkString(", ")}\nFailedAttacks: ${fAttacks.mkString(", ")}"
    writeContentToFile(s"${args(3)}/attacks.txt", content)

    sc.stop()
  }
}