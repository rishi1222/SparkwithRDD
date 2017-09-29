package com.thomsonreuters

import org.apache.jena.rdf.model.{ModelFactory, Model}

/**
 * Created by rishikapoor on 03/04/2017.
 */
class ModelHelper extends Serializable{

     var model:Model = _


  def getModel():Model={

            model = ModelFactory.createDefaultModel

            return model
  }
}
