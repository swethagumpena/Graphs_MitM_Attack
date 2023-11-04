import NetGraphAlgebraDefs.{Action, NodeObject}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.slf4j.LoggerFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import utils.LoadGraph
import utils.FindMatchingElement.matchedElement
import utils.ParseYAML.parseFile
import utils.WriteResults.writeContentToFile
import utils.GraphWalk.randomWalk
import scala.util.Random

object Main {
  private val logger = LoggerFactory.getLogger(getClass)

  // Parse a NodeObject from its string representation
  def parseNodeObject(nodeString: String): NodeObject = {
    // Regular expression pattern to match and extract components of NodeObject
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

  // Generate statistics content for successful and failed attacks
  def statsContent(coverage: Double, totalWalks: Int, successfulAttacks: Set[Int], failedAttacks: Set[Int]): String = {
    s"Coverage: ${coverage * 100}%\nNumber of Random Walks: $totalWalks\nSuccessful Attacks: ${successfulAttacks.mkString(", ")}\nFailed Attacks: ${failedAttacks.mkString(", ")}\nNumber of Successful Attacks: ${successfulAttacks.size}\nNumber of Failed Attacks: ${failedAttacks.size}\n\n"
  }

  def main(args: Array[String]): Unit = {
    logger.info("In spark application")
    val config: Config = ConfigFactory.load("application.conf")

    // Load original and perturbed nodes and edges
    val (originalNodes, originalEdges) = LoadGraph.load(args(0))
    val (perturbedNodes, perturbedEdges) = LoadGraph.load(args(1))

    if (originalNodes.isEmpty || originalEdges.isEmpty || perturbedNodes.isEmpty || perturbedEdges.isEmpty) {
      logger.warn("Input is not of the right format")
    } else logger.info("Graphs successfully loaded")

    // Convert original and perturbed nodes to string representation and parse them
    val originalNodesArray = originalNodes.toArray.map(_.toString)
    val parsedOriginalNodes: Array[NodeObject] = originalNodesArray.map(parseNodeObject)

    logger.info("Storing original node IDs with valuable data")
    val originalNodeIDsWithValuableData: Array[Int] = parsedOriginalNodes.collect {
      case node if node.valuableData => node.id
    }

    val perturbedNodesArray = perturbedNodes.toArray.map(_.toString)
    val parsedPerturbedNodes: Array[NodeObject] = perturbedNodesArray.map(parseNodeObject)

    val isRunningOnAWS = sys.env.contains("AWS_REGION") || sys.env.contains("AWS_EXECUTION_ENV")

    val sc = if (isRunningOnAWS) {
      logger.info("Running application on AWS")
      val spark = SparkSession.builder().appName("Graphs_MitM_Attack").getOrCreate()
      spark.sparkContext
    } else {
      logger.info("Running application locally with 4 cores")
      val conf = new SparkConf().setAppName("RandomWalksApp").setMaster("local[4]") // Set master to local with 4 cores
      new SparkContext(conf)
    }

    // Create an RDD for the nodes
    val nodes: RDD[(VertexId, NodeObject)] = sc.parallelize(parsedPerturbedNodes).map { node =>
      (node.id, node)
    }
    logger.info("Created Nodes RDD")

    // Create an RDD for the edges
    val edges: RDD[Edge[Action]] = sc.parallelize(perturbedEdges.toArray.map(action => Edge(action.fromNode.id, action.toNode.id, action)))
    logger.info("Created Edges RDD")

    val graph = Graph(nodes, edges)
    logger.info("Created GraphX Graph using Nodes RDD and Edges RDD")

    // Parse YAML file for added and modified nodes
    val yamlData = parseFile(args(2))
    val addedNodesList = yamlData("AddedNodes").map(_.toInt)
    val modifiedNodesList = yamlData("ModifiedNodes").map(_.toInt)

    val maxSteps = config.getInt("mitmAttack.maxWalkLength") // Set the maximum number of steps for the random walk
    logger.info(s"Maximum steps for the random walk is $maxSteps")
    val nodesCoverage = config.getDouble("mitmAttack.coverage")
    logger.info(s"Maximum coverage for the graph is $nodesCoverage")

    val visitedNodesAcc = sc.collectionAccumulator[VertexId]("VisitedNodes")

    // Function to process nodes during random walk
    def processNode(vertexId: VertexId, node: NodeObject): (Set[Int], Set[Int]) = {
      val walkScoreTuple = matchedElement(node, parsedOriginalNodes)
      walkScoreTuple.foldLeft((Set.empty[Int], Set.empty[Int])) { case ((successfulAttacks, failedAttacks), (nodeId, perturbedNodeId, _)) =>
        visitedNodesAcc.add(vertexId)
        if (originalNodeIDsWithValuableData.contains(nodeId)) {
          // If it contains valuable data, we attack it
          if (addedNodesList.contains(perturbedNodeId) || modifiedNodesList.contains(nodeId)) {
            // If it is added/modified - leads to a failed attack
            (successfulAttacks, failedAttacks + perturbedNodeId)
          } else {
            // If it is unchanged/removed - leads to a successful attack
            (successfulAttacks + perturbedNodeId, failedAttacks)
          }
        } else {
          // If it does not contain valuable data, we do not attack it
          (successfulAttacks, failedAttacks)
        }
      }
    }

    // List of random starting nodes
    val startNodes = Random.shuffle(graph.vertices.map(_._1).collect().toList)

    val (successfulAttacks, failedAttacks, totalWalks, minNodes, maxNodes, totalNodes, successfulWalksCounter) = startNodes.foldLeft((Set.empty[Int], Set.empty[Int], 0, Int.MaxValue, Int.MinValue, 0, 0)) { case ((sAcc, fAcc, walks, minNodes, maxNodes, totalNodes, successfulWalksCounter), startNode) =>
      val numberOfNodesCovered = visitedNodesAcc.value.toArray.toSet.size
      val numberOfNodesToBeCovered = parsedPerturbedNodes.length * nodesCoverage

      // Condition to stop generating random walks when visited nodes is beyond the threshold
      if (numberOfNodesCovered >= numberOfNodesToBeCovered) {
        (sAcc, fAcc, walks, minNodes, maxNodes, totalNodes, successfulWalksCounter) // Return current state without incrementing walks or counter
      } else {
        val walk = randomWalk(graph, startNode, maxSteps, visitedNodesAcc)
        val (s, f) = walk.flatMap { vertexId =>
          nodes.lookup(vertexId).headOption.map { node =>
            processNode(vertexId, node)
          }
        }.foldLeft((Set.empty[Int], Set.empty[Int])) { case ((s1, f1), (s2, f2)) =>
          (s1 ++ s2, f1 ++ f2)
        }

        val numNodesInWalk = walk.length
        val updatedMinNodes = Math.min(minNodes, numNodesInWalk)
        val updatedMaxNodes = Math.max(maxNodes, numNodesInWalk)
        val updatedTotalNodes = totalNodes + numNodesInWalk
        val updatedSuccessfulWalksCounter = if (s.nonEmpty) successfulWalksCounter + 1 else successfulWalksCounter // Increment when a random walk results in a successful attack

        logger.debug(s"Successfully completed random walk starting from node $startNode")
        logger.trace(s"Number of nodes in this walk: $numNodesInWalk")

        (sAcc ++ s, fAcc ++ f, walks + 1, updatedMinNodes, updatedMaxNodes, updatedTotalNodes, updatedSuccessfulWalksCounter) // Increment walks and update min/max/total nodes, and counter
      }
    }

    val meanNodes = totalNodes.toDouble / totalWalks
    val successfulAttacksRatio = successfulWalksCounter.toDouble / totalWalks

    logger.info(s"Nodes With Valuable Data: ${originalNodeIDsWithValuableData.mkString("", ", ", "")}")
    logger.info(statsContent(nodesCoverage, totalWalks, successfulAttacks, failedAttacks))
    logger.info(s"Minimum Number of Nodes in a Walk: $minNodes")
    logger.info(s"Maximum Number of Nodes in a Walk: $maxNodes")
    logger.info(s"Mean Number of Nodes in a Walk: $meanNodes")
    logger.info(s"Ratio of Number of Random Walks resulting in Successful Attacks to the Total Number of Random Walks: $successfulAttacksRatio")

    // Create content string for the results file
    val content = s"Nodes With Valuable Data: ${originalNodeIDsWithValuableData.mkString("", ", ", "")}\n\n" + statsContent(nodesCoverage, totalWalks, successfulAttacks, failedAttacks) + s"Minimum Number of Nodes in a Walk: $minNodes\nMaximum Number of Nodes in a Walk: $maxNodes\nMean Number of Nodes in a Walk: $meanNodes\nRatio of Number of Random Walks resulting in Successful Attacks to the Total Number of Random Walks: $successfulAttacksRatio"

    // Write content to the results file
    writeContentToFile(s"${args(3)}${config.getString("mitmAttack.resultsFileName")}", content)

    sc.stop()
  }
}