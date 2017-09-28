package com.thomsonreuters

import org.apache._
import org.apache.hadoop.io.LongWritable
import org.apache.jena.hadoop.rdf.types.QuadWritable
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.jena.rdf.model.{ModelFactory, Model}


/**
 * Created by rishikapoor on 24/03/2017.
 */
 class CreateSparkContext {

  def createSparkContext(): SparkContext = {
    var config: SparkConf = null
    try {

      config = new SparkConf()
        .setAppName("SparkqlwithRDD")
        .setMaster("local")
      //.setExecutorEnv("spark.executor.memory ","8g").
      //setExecutorEnv("spark.driver.memory ","8g").setExecutorEnv("spark.shuffle.compress","true").setExecutorEnv("spark.yarn.executor.memoryOverhead","1024")
      //config.set("spark.kryo.registrationRequired", "true")
     config.registerKryoClasses(Array(classOf[hadoop.io.LongWritable], classOf[hadoop.io.Text],classOf[org.apache.jena.rdf.model.Model]
       ,classOf[org.apache.jena.hadoop.rdf.types.QuadWritable],classOf[org.apache.jena.rdf.model.impl.ModelCom]
       ,classOf[org.apache.jena.enhanced.EnhGraph]
       ,classOf[org.apache.jena.rdf.model.ModelFactory]
       ,classOf[org.apache.jena.rdf.model.ModelFactoryBase]))

    } catch {
      case ex: Exception => {
        println("Unable to create Spark Context In Main")
      }
    }
    new SparkContext(config)
  }
}

  object CreateSparkContext {

      private var instance: CreateSparkContext = _;
      def CreateInstance(): CreateSparkContext = {
      instance = new CreateSparkContext()
      return instance

    }
  }




