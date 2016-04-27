package org.personal.durdo.io

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

  // geyser - used for testing?
  val accessKey = "AKIAIQJUGV6J23TG2MAQ"
  val secretKey = "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"

  // maelstrom_dev
  // http://git.visualdna.com/projects/SYS/repos/ansible-playbooks/commits/45ac1f67a6fdec68d6b26bb2c6b1acf19458a4ce#sysops-master/inventories/devel/group_vars/all/vars
  //  val accessKey = "AKIAI7Y77RRZZQOLLYNA"
  //  val secretKey = "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"

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
