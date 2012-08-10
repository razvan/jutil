package com.yieldkit.mongotools.utils

import java.net.URI

import com.mongodb.casbah.Imports._
import com.mongodb.casbah.commons.MongoDBObjectBuilder

import org.apache.hadoop.conf.Configuration

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import org.apache.hadoop.io.Text
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.IOUtils

object Mongo2Seq {

  def export(mongoServer: String, dbName: String, collectionName: String) {

    val mongoConn = MongoConnection(mongoServer)
    val mongoDB = mongoConn(dbName)

    val writer = sequenceFileWriter(collectionName + ".seq")

    val k = new Text()
    val v = new Text()

    for (dbObject <- mongoDB(collectionName)) {
      k.set(dbObject("_id").toString)
      v.set(dbObject.toString)
      writer.append(k, v)
    }
    IOUtils.closeStream(writer)
  }

  def sequenceFileWriter(location: String): SequenceFile.Writer = {
    val conf = new Configuration()
    conf.set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec")
    val fs = FileSystem.get(URI.create(location), conf)
    val path = new Path(location)
    val text = new Text()
    return SequenceFile.createWriter(fs, conf, path, text.getClass(), text.getClass())
  }

  def main(args: Array[String]) {
    if (args.isEmpty) {
      println("USAGE: java -cp ... Mongo2Seq <mongo server> <database> <collection>")
      exit
    }
    export(args(0), args(1), args(2))
  }
}

