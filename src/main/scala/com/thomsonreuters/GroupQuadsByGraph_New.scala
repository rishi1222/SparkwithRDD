package com.thomsonreuters

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

import org.apache.hadoop.io.LongWritable
import org.apache.jena.hadoop.rdf.types.QuadWritable
import org.apache.jena.query._
import org.apache.jena.rdf.model.Model
import org.apache.spark.rdd.RDD

/**
 * Created by rishikapoor on 24/03/2017.
 */
class GroupQuadsByGraph_New extends Serializable {

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

    val queryString2: String = "prefix ont: <http://ont.thomsonreuters.com/>\n" +
      "prefix md: <http://EquitiesDerivativesAndFundsQuote.schemas.financial.thomsonreuters.com/2014-01-01/>\n" +
      "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
      "prefix go: <http://ld.test.data.thomsonreuters.com/>\n" +
      "prefix xml: <xml>\nSELECT ?instrumentId ?davRcsAssetClass ?value ?effectiveFrom ?effectiveTo ?effectiveFromNACode ?effectiveToNACode\n" +
      "WHERE {\n?x rdf:type <http://EquitiesDerivativesAndFundsQuote.schemas.financial.thomsonreuters.com/2014-01-01/Instrument> .\n" +
      "?x ont:permId ?instrumentId .\n" +
      "?x md:davRcsAssetClass ?davRcsAssetClass .\n" +
      "?davRcsAssetClass rdf:value ?value.\n" +
      "OPTIONAL{?davRcsAssetClass md:effectiveFrom ?effectiveFrom} .\n" +
      "OPTIONAL{?davRcsAssetClass md:effectiveTo ?effectiveTo} .\n" +
      "OPTIONAL{?davRcsAssetClass md:effectiveFromNACode ?effectiveFromNACode} .\n" +
      "OPTIONAL{?davRcsAssetClass md:effectiveToNACode ?effectiveToNACode} .\n  " +
      " }"


    val groupByGraph: RDD[(LongWritable, Iterable[QuadWritable])] = readRDD.groupByKey()


    val modelReturned: RDD[String] = groupByGraph.map(x => {

      //Model is defined inside the Map to turn it into RDD
      val model: Model = new ModelHelper().getModel()
      //Nested map statement would take each of Iterable[Quadwritable] and iterate over it
      (x._2).map(quad => {
        //This map function returns RDD of Model
        val triple = quad.get().asTriple()
        val s = model.createResource(triple.getSubject.toString)
        val p = model.createProperty(triple.getPredicate.toString)

        val o =
          if (triple.getObject.isLiteral) {
            model.createLiteral(triple.getObject.toString)
          } else {
            model.createResource(triple.getObject.toString)
          }


        model.add(s, p, o)

      })
      //This map iterates over RDD[Model] and runs SPARQL query over it
      var qExec: QueryExecution = QueryExecutionFactory.create(queryString2, model)
      var results: ResultSet = qExec.execSelect
      //ByteArrayOutputStream is used to store CSV returned fom Resultset Formatter
      val arr: ByteArrayOutputStream = new ByteArrayOutputStream
      var resultSetValues = ResultSetFormatter.outputAsCSV(arr, results)

      //The below staement converts RDD[Iterable(Array[(Byte)]] to  RDD[Iterable[String]]
      new String(arr.toByteArray, StandardCharsets.UTF_8)

    })


    //      The below staement converts RDD[Iterable[String]] to RDD[(String)]
    //   val resutOutput=  modelReturned.flatMap(x=> x.map(x=> (x,1))).reduceByKey(_+_).map(y=>y._1)
    val resutOutput: RDD[String] = modelReturned.map(x => (x, 1)).reduceByKey(_ + _).map(y => y._1)
    resutOutput.saveAsTextFile(output)


    //---------------------------------------------------
  }
}

