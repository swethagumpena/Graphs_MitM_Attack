import NetGraphAlgebraDefs.NodeObject
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}
import com.typesafe.config.{Config, ConfigFactory}
import utils.FindMatchingElement.{jaccardSimilarity, matchedElement}
import utils.{GraphWalk, ParseYAML, WriteResults}
//import org.apache.spark.graphx.{Edge, Graph, VertexId}
//import org.apache.spark.util.CollectionAccumulator
//import NetGraphAlgebraDefs.{Action, NodeObject}
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.rdd.RDD

import java.io.{File, PrintWriter}

class TestCases extends AnyFunSuite with Matchers {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  // MainClass.scala
  test("Check if config loads") {
    // test if the config file loads or not
    val config: Config = ConfigFactory.load("application.conf")
    val mitmConfig = config.getConfig("mitmAttack")
    assert(!mitmConfig.isEmpty)
  }

  // FindMatchingElement.scala - jaccardSimilarity
  test("Check if Jaccard Similarity returns the right result for unequal values negative") {
    val node1: NodeObject = NodeObject(id = 1, children = 1, props = 2, currentDepth = 3, propValueRange = 4, maxDepth = 5, maxBranchingFactor = 6, maxProperties = 7, storedValue = 8.0, valuableData = true)
    val node2: NodeObject = NodeObject(id = 2, children = 1, props = 2, currentDepth = 3, propValueRange = 4, maxDepth = 5, maxBranchingFactor = 9, maxProperties = 10, storedValue = 11.0, valuableData = true)

    val similarity = jaccardSimilarity(node1, node2)
    // intersection is 5 (1,2,3,4,5). Union is 11 (1,2,3,4,5,6,7,8,9,10,11). 5 / 11 ≈ 0.4545
    similarity shouldEqual 0.4545 +- 0.0001
  }

  // FindMatchingElement.scala - jaccardSimilarity
  test("Check if Jaccard Similarity returns the right result for equal values") {
    val node1: NodeObject = NodeObject(id = 1, children = 1, props = 2, currentDepth = 3, propValueRange = 4, maxDepth = 5, maxBranchingFactor = 6, maxProperties = 7, storedValue = 8.0, valuableData = true)
    val node2: NodeObject = NodeObject(id = 2, children = 1, props = 2, currentDepth = 3, propValueRange = 4, maxDepth = 5, maxBranchingFactor = 6, maxProperties = 7, storedValue = 8.0, valuableData = true)

    val similarity = jaccardSimilarity(node1, node2)
    similarity shouldEqual 1.0
  }

  // FindMatchingElement.scala - jaccardSimilarity
  test("Check if Jaccard Similarity returns the right result for all different values") {
    val node1: NodeObject = NodeObject(id = 1, children = 1, props = 2, currentDepth = 3, propValueRange = 4, maxDepth = 5, maxBranchingFactor = 6, maxProperties = 7, storedValue = 8.0, valuableData = true)
    val node2: NodeObject = NodeObject(id = 2, children = 9, props = 10, currentDepth = 11, propValueRange = 12, maxDepth = 13, maxBranchingFactor = 14, maxProperties = 15, storedValue = 16.0, valuableData = false)

    val similarity = jaccardSimilarity(node1, node2)
    similarity shouldEqual 0.0
  }

  // FindMatchingElement.scala - matchedElement
  test("matchedElement should return a tuple if there is a matching element") {
    val node1: NodeObject = NodeObject(id = 1, children = 1, props = 2, currentDepth = 3, propValueRange = 4, maxDepth = 5, maxBranchingFactor = 6, maxProperties = 7, storedValue = 8.0, valuableData = true)
    val node2: NodeObject = NodeObject(id = 2, children = 1, props = 2, currentDepth = 3, propValueRange = 4, maxDepth = 5, maxBranchingFactor = 6, maxProperties = 9, storedValue = 10.0, valuableData = true)

    val originalNodes = Array(node1)
    val perturbedNode = node2

    val result = matchedElement(perturbedNode, originalNodes)
    // node1 and node2 have a similarity of 6/10 = 0.6 Threshold is 0.5.
    // 0.6 > 0.5. So we return Some((originalNode.id, perturbedNode.id, score))
    result shouldBe a[Some[(Int, Int, Double)]]
  }

  // FindMatchingElement.scala - matchedElement
  test("matchedElement should return None for non-matching elements") {
    val node1: NodeObject = NodeObject(id = 1, children = 1, props = 2, currentDepth = 3, propValueRange = 4, maxDepth = 5, maxBranchingFactor = 6, maxProperties = 7, storedValue = 8.0, valuableData = true)
    val node2: NodeObject = NodeObject(id = 2, children = 1, props = 2, currentDepth = 3, propValueRange = 4, maxDepth = 5, maxBranchingFactor = 9, maxProperties = 10, storedValue = 11.0, valuableData = true)

    val originalNodes = Array(node1)
    val perturbedNode = node2

    val result = matchedElement(perturbedNode, originalNodes)
    // node1 and node2 have a similarity of 5/11 = 0.4545 Threshold is 0.5.
    // 0.4545 < 0.5. So we return None
    result shouldBe None
  }

  // ParseYAML.scala - parseFile
  test("'parseFile' should return a Map with the extracted information") {
    val testFilePath = "src/test/scala/resources/testFile.txt"
    val testFileContent =
      """|Nodes:
         |	Modified: [12, 8]
         |	Removed: [7, 3]
         |	Added:
         |		3: 14
         |		4: 13
         |Edges:
         |	Modified:
         |		4: 5
         |	Added:
         |		3: 14
         |		4: 13
         |	Removed:
         |		7: 9
         |""".stripMargin
    val writer = new PrintWriter(testFilePath)
    writer.println(testFileContent)
    writer.close()

    val result = ParseYAML.parseFile(testFilePath)

    // Check if the result contains the expected information
    result should contain key "AddedNodes"
    result should contain key "ModifiedNodes"
    result should contain key "RemovedNodes"
    result should contain key "AddedEdges"
    result should contain key "ModifiedEdges"
    result should contain key "RemovedEdges"

    result("AddedNodes") should contain theSameElementsAs List("14", "13")
    result("ModifiedNodes") should contain theSameElementsAs List("12", "8")
    result("RemovedNodes") should contain theSameElementsAs List("7", "3")
    result("AddedEdges") should contain theSameElementsAs List("3-14", "4-13")
    result("ModifiedEdges") should contain theSameElementsAs List("4-5")
    result("RemovedEdges") should contain theSameElementsAs List("7-9")
  }

  // ParseYAML.scala - parseFile
  test("should return an empty Map for an invalid file path or URL") {
    val invalidPath = "invalidPathOrURL"
    val result = ParseYAML.parseFile(invalidPath)
    result shouldBe empty
  }

  // WriteResults.scala - getBucketName
  test("getBucketName should extract bucket name from S3 path") {
    val s3Path = "s3://my-bucket/my-file.txt"
    val bucketName = WriteResults.getBucketName(s3Path)
    assert(bucketName == "my-bucket")
  }

  // WriteResults.scala - getKey
  test("getKey should extract key from S3 path") {
    val s3Path = "s3://my-bucket/my-file.txt"
    val key = WriteResults.getKey(s3Path)
    assert(key == "my-file.txt")
  }
}