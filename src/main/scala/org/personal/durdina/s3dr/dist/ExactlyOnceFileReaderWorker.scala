package org.personal.durdina.s3dr.dist

import java.io.File
import java.util.concurrent.TimeUnit

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.locks.{InterProcessLock, InterProcessSemaphoreMutex}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.personal.durdina.s3dr.io.{S3Client, S3File}

import scala.collection._

/**
  * Created by misko on 26/04/2016.
  */
object ExactlyOnceFileReaderWorker {

  // S3 stuff
  val s3Client = new S3Client()
  val FileNameTemplate = "data-{id}.txt"
  val ProcessedFlagPrefix = "PROCESSED-"
  val Bucket = "dev.eu-west-1.s3reader"

  // Curator/Zookeeper stuff
  val LockPath: String = "/distprim"
  val ZookeeperConnectionString: String = "localhost:2181"
  val curatorClient = CuratorFrameworkFactory.newClient(ZookeeperConnectionString, new ExponentialBackoffRetry(1000, 3))
  curatorClient.start()

  // List of processed files
  type TimeStamp = Long
  type FileName = String
  val processed = mutable.ArrayBuffer.empty[(TimeStamp, FileName)]

  def main(args: Array[String]) {
    val argMap: Map[String, String] = createArgMap(args)
    val id = argMap("id").toInt
    val total = argMap("total").toInt // TODO: distribute the total number of instances automatically by lookup into registry (zookeeper) - distributed counter
    println(s"Process id=$id from group of $total processes starts")

    val processables = s3Client.listOfFiles(new S3File(Bucket, "data-"))

    var currentIx = ((processables.size.toDouble / total) * (id - 1)).ceil.toInt
    val finishIx = currentIx - 1 + processables.size

    println(s"Process id=$id starts processing from $currentIx up to ${finishIx % processables.size}")

    // TODO: rework to immutable (using recursion)
    while (currentIx <= finishIx) {
      // try to acquire lock for a file - skip if not possible (other process working on that file)
      val processable = processables(currentIx % processables.size)
      val lock: InterProcessLock = new InterProcessSemaphoreMutex(curatorClient, s"$LockPath/$processable")
      if (lock.acquire(0, TimeUnit.SECONDS)) {
        try {
          // do some work inside of the critical section here
          if (argMap.contains("normal"))
            processRunsNormally(id, new S3File(Bucket, processable))
          else if (argMap.contains("fail"))
            processFail(id)
          else if (argMap.contains("forever"))
            processRunsEndlessly(id)
          else {
            Console.err.print("Missing mode parameter - please add on of normal|fail|run-forever")
            System.exit(2)
          }
        } finally {
          // TODO: should not release before we can be sure that all other S3 instances see the flag file (maybe this guarantee cannot be made and the lock should remain with some looong TTL that would minimise the chance that a flag file would not be seen)
          lock.release()
        }
      }
      currentIx += 1
    }

    processed.foreach {
      case (ts, msg) => println(s"[$ts, $msg]")
    }
    println(s"Process id=$id finishes")
  }

  private def processRunsNormally(id: Int, processable: S3File): Unit = {
    println(s"Process id=$id does the work on $processable")

    val processedFlagFileName = ProcessedFlagPrefix + processable.key
    val processedFlagFile = new S3File(Bucket, processedFlagFileName)
    val processedFlagTemplateFile = new File(getClass.getResource("/" + ProcessedFlagPrefix + FileNameTemplate).toURI)

    val files = s3Client.listOfFiles(processedFlagFile)
    // if the flag file does not exist yet
    if (files.isEmpty) {
      // mark processable as processed
      s3Client.upload(processedFlagTemplateFile, processedFlagFile)
      processed += ((System.currentTimeMillis(), processable.key))
    }
  }

  private def processRunsEndlessly(id: Int) {
    println(s"Process id=$id WILL run forever")
    while (true) TimeUnit.SECONDS.sleep(5)
  }

  private def processFail(id: Int) {
    println(s"Process id=$id FAILs deliberately and forcefully")
    System.exit(1)
  }

  private def createArgMap(args: Array[String]): Map[String, String] =
    Set(args: _*).map(_.split("=")).map {
      case Array(k) => (k, null)
      case Array(k, v) => (k, v)
    }.toMap
}
