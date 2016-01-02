package com.nuvostaq.bigdataspark

import java.io.{FileInputStream, ByteArrayOutputStream, IOException}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

/**
  * Created by juha on 2.1.2016.
  * (c) 2016 Nuvostaq Oy
  */
object Util {
  @throws(classOf[IOException])
  def compressString(data: String): Array[Byte] =
  {
    val bos: ByteArrayOutputStream = new ByteArrayOutputStream(data.length)
    val gzip: GZIPOutputStream = new GZIPOutputStream(bos)
    gzip.write(data.getBytes)
    gzip.close()
    val compressed: Array[Byte] = bos.toByteArray
    bos.close()
    compressed
  }
  @throws(classOf[IOException])
  def readAndDecompressFile(fileName: String): Seq[String] =
  {
    val fileStream = new FileInputStream(fileName)
    val gzis = new GZIPInputStream(fileStream)
    scala.io.Source.fromInputStream(gzis).mkString.split("\n")
  }
}
