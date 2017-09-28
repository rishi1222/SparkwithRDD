package com.thomsonreuters

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

/**
 * Created by rishikapoor on 24/03/2017.
 */
class AbstractSparkContext {
  var fs:FileSystem = _
  val sc = CreateSparkContext.CreateInstance().createSparkContext()
  var conf:Configuration = new Configuration()
  if("local".contains("hdfs")) {
    fs = FileSystem.get(conf)
  }else {
    fs = FileSystem.getLocal(conf)
  }

}
