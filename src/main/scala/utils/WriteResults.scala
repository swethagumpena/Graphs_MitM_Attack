package utils

import java.io.PrintWriter
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{PutObjectRequest, S3Exception}
import software.amazon.awssdk.regions.Region

object WriteResults {
  /**
   * Writes content to an S3 object or local file.
   *
   * @param filePath The path (S3 or local) to write to.
   * @param content  The content to write.
   */
  def writeContentToFile(filePath: String, content: String): Unit = {
    if (filePath.startsWith("s3://")) {
      writeToS3(filePath, content)
    } else {
      writeToLocal(filePath, content)
    }
  }

  /**
   * Writes content to an S3 object.
   *
   * @param filePath The S3 path (s3://bucketName/fileName).
   * @param content  The content to write.
   */
  private def writeToS3(filePath: String, content: String): Unit = {
    val s3Client = S3Client.builder().region(Region.US_EAST_1).build()

    try {
      val inputStream = new java.io.ByteArrayInputStream(content.getBytes("UTF-8"))
      val requestBody = RequestBody.fromInputStream(inputStream, content.length())

      val bucketName = getBucketName(filePath)
      val key = getKey(filePath)

      val request = PutObjectRequest.builder()
        .bucket(bucketName)
        .key(key)
        .contentType("text/plain")
        .build()

      s3Client.putObject(request, requestBody)
    } catch {
      case e: S3Exception => e.printStackTrace()
    } finally {
      s3Client.close()
    }
  }

  /**
   * Writes content to a local file.
   *
   * @param filePath The path to the local file.
   * @param content  The content to write.
   */
  private def writeToLocal(filePath: String, content: String): Unit = {
    val writer = new PrintWriter(filePath)
    writer.println(content)
    writer.close()
  }


  def getBucketName(s3Path: String): String = {
    s3Path.drop(5).takeWhile(_ != '/')
  }

  def getKey(s3Path: String): String = {
    s3Path.drop(5 + getBucketName(s3Path).length + 1)
  }
}
