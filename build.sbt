name := "SparkwithRDD"

version := "1.0"

scalaVersion := "2.10.5"

resolvers ++= Seq(
  "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Artima Maven Repository" at "http://repo.artima.com/releases/"
)

val hadoopVersion = "2.6.0-cdh5.5.2"
val sparkVersion = "1.5.0-cdh5.5.2"


libraryDependencies += "org.apache.jena" % "jena-elephas-io" % "3.0.0"

val hadoopClientExcludes =
  (moduleId: ModuleID) => moduleId.
    exclude("org.slf4j", "slf4j-api").
    exclude("javax.servlet", "servlet-api").
    exclude("com.esotericsoftware.minlog", "minlog").
    exclude("org.eclipse.jetty.orbit", "javax.servlet")

val sparkExcludes =
  (moduleId: ModuleID) => moduleId.
    exclude("org.apache.hadoop", "hadoop-client").
    exclude("org.apache.hadoop", "hadoop-yarn-client").
    exclude("org.apache.hadoop", "hadoop-yarn-api").
    exclude("org.apache.hadoop", "hadoop-yarn-common").
    exclude("org.apache.hadoop", "hadoop-yarn-server-common").
    exclude("com.esotericsoftware.minlog", "minlog").
    //exclude("org.eclipse.jetty.orbit", "javax.servlet")
    exclude("org.apache.hadoop", "hadoop-yarn-server-web-proxy")

val isALibrary = false //this is a library project
lazy val hadoopDependenciesScope = if (isALibrary) "provided" else "compile"

libraryDependencies ++= Seq(
  //% method creates moduleId
  sparkExcludes("org.apache.spark" %% "spark-core" % sparkVersion % "compile"),
  sparkExcludes("org.apache.spark" %% "spark-sql" % sparkVersion % "compile"),
  sparkExcludes("org.apache.spark" %% "spark-yarn" % sparkVersion % "compile"),
  sparkExcludes("org.apache.spark" %% "spark-mllib" % sparkVersion % "compile"),
  sparkExcludes("org.apache.spark" %% "spark-streaming" % sparkVersion % "compile"),
  sparkExcludes("org.apache.spark" %% "spark-network-common" % sparkVersion % "compile"),
  sparkExcludes("org.apache.spark" %% "spark-network-yarn" % sparkVersion % "compile"),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-yarn-api" % hadoopVersion % hadoopDependenciesScope),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-yarn-client" % hadoopVersion % hadoopDependenciesScope),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-yarn-common" % hadoopVersion % hadoopDependenciesScope),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-yarn-server-web-proxy" % hadoopVersion % hadoopDependenciesScope),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-yarn-applications-distributedshell"% hadoopVersion % hadoopDependenciesScope),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-client"% hadoopVersion % hadoopDependenciesScope),
  hadoopClientExcludes("org.apache.hadoop" % "hadoop-yarn-registry"% hadoopVersion % hadoopDependenciesScope)
  ,hadoopClientExcludes("org.apache.hadoop" % "hadoop-mapreduce-client-common"% hadoopVersion % hadoopDependenciesScope)
  ,hadoopClientExcludes("org.apache.hadoop" % "hadoop-common"% hadoopVersion % hadoopDependenciesScope)
)