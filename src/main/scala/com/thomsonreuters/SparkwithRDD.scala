package com.thomsonreuters

import org.apache.hadoop.conf.Configuration

import org.apache.hadoop
import org.apache.hadoop.io.LongWritable
import org.apache.jena.hadoop.rdf.io.input.TriplesOrQuadsInputFormat
import org.apache.jena.hadoop.rdf.types.QuadWritable
import org.apache.jena.query.{ResultSet, QueryExecutionFactory, QueryExecution}
import org.apache.jena.rdf.model.{ModelFactory, Model}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by rishikapoor on 24/03/2017.
 */
object SparkwithRDD {



  def main(args :Array[String]) :Unit={
   new ReadRDF().readRDF(args{0},args{1},args{2})
  }




}
