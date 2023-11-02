import NetGraphAlgebraDefs.{Action, NodeObject}
import com.typesafe.config.{Config, ConfigFactory}
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
import utils.GraphWalk.randomWalk
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

  def main(args: Array[String]): Unit = {
    logger.info("In spark application")
    val config: Config = ConfigFactory.load("application.conf")

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

    val maxSteps = config.getInt("mitmAttack.maxWalkLength") // Set the maximum number of steps for the random walk

    val nodesCoverage = config.getDouble("mitmAttack.coverage")

    // Define an accumulator for visited nodes
    val visitedNodesAcc = sc.collectionAccumulator[VertexId]("VisitedNodes")

    // Function to perform random walks from randomly selected nodes
    def performRandomWalks(graph: Graph[NodeObject, Action], maxSteps: Int, visitedNodesAcc: CollectionAccumulator[VertexId]): Unit = {
      def processNode(vertexId: VertexId, node: NodeObject): Unit = {
        val walkScoreTuple = calculateScore(node, parsedOriginalNodes)
        walkScoreTuple.foreach { walkScoreTuple =>
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

      val startNodes = Random.shuffle(graph.vertices.map(_._1).collect().toList)
      startNodes.foreach { startNode =>
        if (visitedNodesAcc.value.toArray.toSet.size >= parsedPerturbedNodes.length * nodesCoverage) return
        val walk = randomWalk(graph, startNode, maxSteps, visitedNodesAcc)
        walk.foreach { vertexId =>
          nodes.lookup(vertexId).headOption.foreach { node =>
            processNode(vertexId, node)
          }
        }
      }
    }

    // Perform random walks
    performRandomWalks(graph, maxSteps, visitedNodesAcc)

    val content = s"NodesWithValuableData: ${originalNodeIDsWithValuableData.mkString("", ", ", "")}\nSuccessfulAttacks: ${sAttacks.mkString(", ")}\nFailedAttacks: ${fAttacks.mkString(", ")}"
    writeContentToFile(s"${args(3)}/${config.getString("mitmAttack.resultsFileName")}", content)

    sc.stop()
  }
}