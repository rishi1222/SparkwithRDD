package com.thomsonreuters

/**
 * Created by rishikapoor on 29/03/2017.
 */
import org.apache.jena.query.{ARQ, QueryFactory}

import scala.collection.JavaConversions._

object SparqlInspector extends Serializable{

  def findResultVariables(queryString: String): Vector[String] = {
    ARQ.init()
    val query = QueryFactory.create(queryString)
    if (query.isSelectType()) {
      query.getResultVars.toVector
    } else if (query.isConstructType() || query.isDescribeType()) {
      Vector("Subject", "Predicate", "Object")
    } else if (query.isAskType()) {
      Vector("ASK")
    } else {
      query.getResultVars.toVector
    }
  }

}