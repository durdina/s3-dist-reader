package org.personal.durdina.s3dr

import org.personal.durdina.s3dr.dist.ExactlyOnceFileReaderWorker
import org.personal.durdina.s3dr.io.S3Content

/**
  * Created by misko on 25/04/2016.
  */
object Main {

  def main(args: Array[String]) {
    println("STARTED MAIN")

    S3Content.initialize(10)

    val p1 = new ProcessRunner(ExactlyOnceFileReaderWorker.getClass, "id=1", "total=4", "normal").run()
    val p2 = new ProcessRunner(ExactlyOnceFileReaderWorker.getClass, "id=2", "total=4", "normal").run()
    val p3 = new ProcessRunner(ExactlyOnceFileReaderWorker.getClass, "id=3", "total=4", "fail").run()
    val p4 = new ProcessRunner(ExactlyOnceFileReaderWorker.getClass, "id=4", "total=4", "normal").run()

    p1.printout()
    p2.printout()
    p3.printout()
    p4.printout()

    // TODO: transform to test
    (p1.logs ++ p2.logs ++ p3.logs ++ p4.logs).sorted.foreach(println)

    println("FINISHED MAIN")
  }

}
