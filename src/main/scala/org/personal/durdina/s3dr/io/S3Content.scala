package org.personal.durdina.s3dr.io

import java.io.File


/**
  * Created by misko on 27/04/2016.
  */
object S3Content {

  // TODO move to constants
  val FileNameTemplate = "data-{id}.txt"
  val Bucket = "dev.eu-west-1.s3reader"
  val s3Client = new S3Client()

  def initialize(numFiles: Integer) = {
    s3Client.emptyBucket(Bucket)

    val sampleFile = getClass.getResource("/" + FileNameTemplate)
    val file = new File(sampleFile.toURI)
    for (i <- 0 to numFiles - 1) {
      val fileNameSubst = FileNameTemplate.replaceAll("\\{.*\\}", f"$i%02d")
      s3Client.upload(file, new S3File(Bucket, fileNameSubst))
    }
  }

}
