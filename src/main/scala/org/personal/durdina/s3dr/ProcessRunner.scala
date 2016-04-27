package org.personal.durdina.s3dr

import java.io.{File, InputStream, StringWriter}
import java.net.{URLClassLoader, URLDecoder}

import scala.io.Source

/**
  * Created by misko on 26/04/2016.
  */
class ProcessRunner[T](val clazz: Class[T], args: String*) {

  val javaExecutable = System.getProperty("java.home") + "/bin/java"
  val urls = Thread.currentThread().getContextClassLoader.asInstanceOf[URLClassLoader].getURLs
  val classpath = URLDecoder.decode(urls.mkString(File.pathSeparator).replaceAll("%20", " "), "UTF-8")

  var process: Process = _

  def run() = {
    val workerClass = clazz.getName.takeWhile(_ != '$')

    val cmdLine = Array(javaExecutable, "-cp", classpath, workerClass) ++ args
    val processBuilder = new ProcessBuilder(cmdLine: _*)
    process = processBuilder.start()
    this
  }

  def isAlive = process.isAlive

  def rc = process.exitValue()

  def printout() = {
    val rc = process.waitFor()
    println(if (rc == 0) output else output + error)
  }

  def logs(): List[(Long, String, String)] = {
    val TsMsgPattern = """\[(\d*),(.*)\]""".r
    Source.fromString(output)
      .getLines
      .collect { case TsMsgPattern(ts, msg) => (ts.toLong, msg, args.mkString(" ")) }
      .toList
  }

  // TODO: implement thread reading from process inputStream and asynchronously writing to the console
  lazy val output = {
    process.waitFor()
    readStream(process.getInputStream)
  }

  lazy val error = {
    process.waitFor()
    readStream(process.getErrorStream)
  }

  private def readStream(is: InputStream) = {
    val s = new java.util.Scanner(is).useDelimiter("\\A")

    val sw = new StringWriter()
    while (s.hasNext) sw.write(s.next())
    sw.toString
  }

}
