package com.yieldkit
package mongotools.utils

import com.mongodb.casbah.Imports._
import com.mongodb.casbah.commons.MongoDBObjectBuilder

object PageKeywordsProcessor {

  val DB_NAME = "lovedby-bak"

  val mongoConn = MongoConnection()
  val mongoDB = mongoConn(DB_NAME)
  val aaliColl = mongoConn(DB_NAME)("AssignableAffiliateLinkImpl")
  val q : MongoDBObject = ("pageKeywords.html" $exists true ) ++ ("affiliateNetwork" -> "ZANOX")
  val pageKeywords = aaliColl.find(q)

  val pkcColl = mongoConn(DB_NAME)("PageKeywordsCount")

  def countPageKeywords =
    aaliColl.find(q).foreach {
      aali => {
        val builder = new MongoDBObjectBuilder
        builder += "assignableLinkId" -> aali._id.get
        val tokenMap = countTokensInPageKeywords(aali).foreach {
            t => if (t._2 > 1) builder += t._1 -> t._2
        }

        // only save when we had tokens with > 1 freq
        if (1 < builder.result.size)
          pkcColl += builder.result
      }
    }

  def countTokensInPageKeywords(aali: DBObject) : Map[String, Int] = {
    val pk = aali.getAs[BasicDBObject]("pageKeywords").get

    countKeywordTokensInHtml(
      uniqueTokens(keywordValues(pk)),
      pk.getAs[String]("html").get)
  }

  private val kFields = List("description","keywords")
  private def keywordValues(o: BasicDBObject): List[String] = {
    kFields.filter(o.containsField _).map(o.getAs[String](_).get)
  }

  private def uniqueTokens(values: List[String]) : Set[String] = {
    val ll = for {
      v <- values
      tlist = v.split("\\b").filter(t => t.matches("[A-Z]\\w+"))
    } yield tlist

    ll.flatten.toSet
  }

  private def countKeywordTokensInHtml(tokenSet: Set[String], html: String) =
    html
      .split("\\b")
      .filter(t => t.matches("\\w+"))
      .foldLeft(Map[String,Int]())( (m,t) => m + (t -> (m.getOrElse(t,0) + 1)) )
      .filterKeys(k => tokenSet.contains(k))
}
