package com.thomsonreuters

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.util

import breeze.linalg.split
import org.apache.hadoop.io.LongWritable
import org.apache.jena.graph.Node
import org.apache.jena.hadoop.rdf.types.QuadWritable
import org.apache.jena.query._
import org.apache.jena.rdf.model.{RDFNode, ModelFactory, Model}
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by rishikapoor on 24/03/2017.
 */
class GroupQuadsByGraph extends Serializable {

  // var createModelRdd: CreateModelRDD = new CreateModelRDD()


  def splitRDFrdd(readRDD: RDD[(LongWritable, QuadWritable)], output: String, styleSheet: String): Unit = {


    //
    //    val reduceQuadByKey:RDD[Model]= readRDD.map(m => (m._2, 1)).reduceByKey(_ + _).map(quad=>{
    //    //reduceQuadByKey.foreach(println)
    //    val sb:Model = new ModelHelper().getModel()
    //    val triple = quad._1.get().asTriple()
    //      val s = sb.createResource(triple.getSubject.toString)
    //      val p = sb.createProperty(triple.getPredicate.toString)
    //
    //      val o =
    //        if (triple.getObject.isLiteral) {
    //          sb.createLiteral(triple.getObject.toString)
    //        } else {
    //          sb.createResource(triple.getObject.toString)
    //        }
    //    sb.add(s,p,o)
    //    })
    //
    //
    //    //reduceQuadByKey.foreach(println)
    //    createModelRdd.createModel(reduceQuadByKey)
    //
    //  }


    //      val dataset:Dataset = DatasetFactory.createMem()
    //      val mergeModel:Model = dataset.getDefaultModel
    //      var mergedModelRetuned:RDD[Model] = modelReturned.map(m => {
    //        mergeModel.add(m)
    //      })

    //
    //            modelReturned.map(x=>{
    //              //This map iterates over RDD[Model] and runs SPARQL query over it
    //              var qExec: QueryExecution = QueryExecutionFactory.create(queryString1, x)
    //              var results: ResultSet = qExec.execSelect
    //              //ByteArrayOutputStream is used to store CSV returned fom Resultset Formatter
    //              val arr: ByteArrayOutputStream = new ByteArrayOutputStream
    //              //var resultSetValues = ResultSetFormatter.outputAsCSV(arr, results)
    //              var resultSetValues = ResultSetFormatter.out(results)
    //              println(resultSetValues)
    //
    //              //The below staement converts RDD[Iterable(Array[(Byte)]] to  RDD[Iterable[String]]
    //              //new String(arr.toByteArray, StandardCharsets.UTF_8)
    //
    //            })

    //  ----------------------------------------

    val queryString2: String = "prefix ns0: <http://ontology.thomsonreuters.com/Deals/DealsAuxiliary/>\n" +
      "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
      "prefix xsd: <http://www.w3.org/2001/XMLSchema#>\nprefix xml: <xml> \n" +
      "SELECT ?AUXENTITYNUMBER ?AUXENTITYTYPE ?LANGUAGEID \n" +
      "WHERE {\n" +
      "OPTIONAL {?x rdf:type <http://ontology.thomsonreuters.com/Deals/DealsAuxiliary/EntityNames>} .\n" +
      "?x  ns0:AUXENTITYNUMBER ?AUXENTITYNUMBER.\n" +
      "?x  ns0:AUXENTITYTYPE ?AUXENTITYTYPE .\n" +
      "OPTIONAL {?x  ns0:LANGUAGEID ?LANGUAGEID}.\n" +
      "}"

    val groupByGraph:RDD[(String, Iterable[QuadWritable])] = readRDD.map(x=>(x._2)).map(x=>(x.get().getSubject.toString,x)).groupByKey()


    val modelReturned: RDD[String] = groupByGraph.map(x => {

      //Model is defined inside the Map to turn it into RDD
      val model: Model = ModelFactory.createDefaultModel
      var stringBuffer = new StringBuilder

      //Nested map statement would take each of Iterable[Quadwritable] and iterate over it
      (x._2).foreach( quad => {
      //This map function returns RDD of Model

      //        stringBuffer.append(quad.__leftOfArrow)
      //        stringBuffer.append("\n")
      val triple = quad.get().asTriple()
      var s: org.apache.jena.rdf.model.Resource = model.createResource(triple.getSubject.toString)
      var p: org.apache.jena.rdf.model.Property = model.createProperty(triple.getPredicate.toString)

      var o: org.apache.jena.rdf.model.RDFNode =
        if (triple.getObject.isLiteral) {
          model.createLiteral(triple.getObject.toString)
        } else {
          model.createResource(triple.getObject.toString)
        }

      model.add(s, p, o)
    })



      //This map iterates over RDD[Model] and runs SPARQL query over it
      val query2: Query = QueryFactory.create(queryString2)
      val qExec: QueryExecution = QueryExecutionFactory.create(query2, model)
      val results = qExec.execSelect

      while(results.hasNext){
        var value = results.nextSolution()
        var varNames = value.varNames()
        while(varNames.hasNext){
          var variable = varNames.next()
          var varNode = value.get(variable).asNode()
          if(varNode.isURI){
            stringBuffer.append(varNode.getURI)
            stringBuffer.append(',')
          }else if(varNode.isLiteral){
            stringBuffer.append(varNode.getLiteralLexicalForm)
            stringBuffer.append(',')
          }else if(varNode.isVariable){
            stringBuffer.append(value.get(varNode.getName))
            stringBuffer.append(',')
          }


        }
        //println("-------------")
        //stringBuffer.append("\n")
      }
      stringBuffer.toString()


    })
//
//
//    //      The below staement converts RDD[Iterable[String]] to RDD[(String)]
//       //val resutOutput=  modelReturned.flatMap(x=> x.map(x=> (x,1))).reduceByKey(_+_).map(y=>y._1)
    val resutOutput: RDD[String] = modelReturned.map(x => (x, 1)).reduceByKey(_ + _).map(y=>y._1)
    resutOutput.saveAsTextFile(output)

    //modelReturned.map(x => x.getResourceModel ).saveAsTextFile(output)


    //---------------------------------------------------
  }
}

