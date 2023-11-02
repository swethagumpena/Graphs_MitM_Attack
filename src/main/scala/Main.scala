import org.slf4j.LoggerFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import utils.LoadGraph

object Main {
  private val logger = LoggerFactory.getLogger(getClass)

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
  }
}