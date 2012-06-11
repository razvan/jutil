package com.yieldkit
package mongotools.utils

import java.net.URI

import com.mongodb.casbah.Imports._
import com.mongodb.casbah.commons.MongoDBObjectBuilder

import org.apache.hadoop.conf.Configuration

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import org.apache.hadoop.io.Text
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.IOUtils

import net.htmlparser.jericho.Source

case class PageKeywords(val url: String, val content: String)

object PageKeywords2SequenceFile {

  val DB_NAME = "lovedby-bak"

  val mongoConn = MongoConnection()
  val mongoDB = mongoConn(DB_NAME)
  val aaliColl = mongoConn(DB_NAME)("AssignableAffiliateLinkImpl")
  val q : MongoDBObject = ("pageKeywords.html" $exists true ) ++ ("affiliateNetwork" -> "ZANOX")
  val pageKeywordsCursor = aaliColl.find(q)

  def export() {
    val writer = sequenceFileWriter("/tmp/yieldkit/seq/PageKeywords.seq")
    val k = new Text()
    val v = new Text()
    pageKeywordsCursor.foreach {
      aali =>
        val pk = getPageKeywords(aali)
        k.set(pk.url)
        v.set(pk.content)
        writer.append(k, v)
    }
    IOUtils.closeStream(writer)
  }

  def sequenceFileWriter(location: String): SequenceFile.Writer = {
    val conf = new Configuration()
    val fs = FileSystem.get(URI.create(location), conf)
    val path = new Path(location)
    val text = new Text()
    return SequenceFile.createWriter(fs, conf, path, text.getClass(), text.getClass())
  }

  def getPageKeywords(o: DBObject): PageKeywords = {
    val pk = o.getAs[BasicDBObject]("pageKeywords").get

    return PageKeywords(pk.getAs[String]("targetUrl").get, extractContent(o.getAs[String]("html").get))
  }

  def extractContent(html: String):String = {
    val source = new Source(html)
    source.fullSequentialParse()
    source.getTextExtractor.toString
  }
}

