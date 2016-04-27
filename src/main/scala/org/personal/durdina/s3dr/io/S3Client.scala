package org.personal.durdina.s3dr.io

import java.io.File

import com.amazonaws.auth.{AWSCredentials, BasicAWSCredentials}
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model._

import scala.annotation.tailrec
import scala.collection.JavaConverters._

/**
  * Created by misko on 26/04/2016.
  */
class S3Client {

  // TODO: externalize
  val accessKey = "AKIAIM4TY5PUZ3VXMDKA"
  val secretKey = "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"

  val credentials: AWSCredentials = new BasicAWSCredentials(accessKey, secretKey)
  val s3client = new AmazonS3Client(credentials)

  def upload(source: File, target: S3File) {
    val request = new PutObjectRequest(target.bucket, target.key, source)
    s3client.putObject(request)
  }

  def listOfFiles(prefix: S3File) = {
    listObjects(prefix.bucket, prefix.key, _.getKey)
  }

  def emptyBucket(bucket: String) {
    listOfFiles(new S3File(bucket, "")).foreach(s3client.deleteObject(bucket, _))
  }

  private def listObjects[T](bucket: String, keyPrefix: String, transformFunc: S3ObjectSummary => T): Seq[T] = {
    @tailrec
    def aggregateObjects(objectListing: ObjectListing, files: List[S3ObjectSummary]): List[S3ObjectSummary] = {
      if (!objectListing.isTruncated)
        files ++ objectListing.getObjectSummaries.asScala
      else
        aggregateObjects(s3client.listNextBatchOfObjects(objectListing), files ++ objectListing.getObjectSummaries.asScala)
    }

    val files = aggregateObjects(s3client.listObjects(bucket, keyPrefix), Nil)
    files.map(transformFunc)
  }

}
