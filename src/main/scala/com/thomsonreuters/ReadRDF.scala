package com.thomsonreuters

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.LongWritable
import org.apache.jena.hadoop.rdf.io.input.{TriplesOrQuadsInputFormat, QuadsInputFormat}
import org.apache.jena.hadoop.rdf.types.QuadWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by rishikapoor on 24/03/2017.
 */
class ReadRDF  extends AbstractSparkContext {

  val groupQuadObject = new GroupQuadsByGraph

  def readRDF(inputFile:String,output:String,styleSheet:String):Unit = {

    println(inputFile)

    try {

      val readRDD = sc.newAPIHadoopFile(inputFile,
        classOf[QuadsInputFormat],
        classOf[LongWritable], //position
        classOf[QuadWritable], //value
        conf)

      callGroupQuad(readRDD,output,styleSheet)
    } catch {
      case ex:Exception => {
        println("unable to create RDD")
        throw new Exception
      }

    }

    def callGroupQuad(readRDD:RDD[(LongWritable,QuadWritable)],output:String,styleSheet:String):Unit={

      groupQuadObject.splitRDFrdd(readRDD,output,styleSheet)
    }

  }



}
