name := "wikidig"
version := "0.2"

scalaVersion := "2.13.1"
sbtVersion := "1.3.2"
scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")
javacOptions in(Compile, compile) ++= Seq("-source", "1.8",
  "-encoding", "UTF-8",
  "-target", "1.8", "-g:lines")

lazy val root = (project in file(".")).
  enablePlugins(BuildInfoPlugin).
  settings(
    buildInfoKeys := BuildInfoKey.ofN(name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "wiki.dig.common"
  )

buildInfoKeys ++= Seq[BuildInfoKey](
  "author" -> "XiaTian"
)

buildInfoKeys += buildInfoBuildNumber
buildInfoOptions += BuildInfoOption.BuildTime
buildInfoOptions += BuildInfoOption.ToJson

libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "1.2.0"

libraryDependencies += "com.typesafe" % "config" % "1.3.3"

//command line parser
libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.0-RC2"

//Scala wrapper for Joda Time.
libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.22.0"

//Scala better file
libraryDependencies += "com.github.pathikrit" %% "better-files" % "3.8.0"

// Google Guava
libraryDependencies += "com.google.guava" % "guava" % "28.1-jre"

//database driver
libraryDependencies += "org.rocksdb" % "rocksdbjni" % "6.2.2"
libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.23"

//spark web
libraryDependencies += "com.sparkjava" % "spark-core" % "2.9.1"

//slick database process
libraryDependencies += "com.typesafe.slick" %% "slick" % "3.3.2"
libraryDependencies += "com.typesafe.slick" %% "slick-hikaricp" % "3.3.2"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.2"

//NLP libraries
libraryDependencies += "com.hankcs" % "hanlp" % "portable-1.7.5"
libraryDependencies += "org.ahocorasick" % "ahocorasick" % "0.3.0"

//JWPL
libraryDependencies += "de.tudarmstadt.ukp.wikipedia" % "de.tudarmstadt.ukp.wikipedia.api" % "1.1.0"
libraryDependencies += "de.tudarmstadt.ukp.wikipedia" % "de.tudarmstadt.ukp.wikipedia.datamachine" % "1.1.0"
libraryDependencies += "de.tudarmstadt.ukp.wikipedia" % "de.tudarmstadt.ukp.wikipedia.util" % "1.1.0"
libraryDependencies += "de.tudarmstadt.ukp.wikipedia" % "de.tudarmstadt.ukp.wikipedia.parser" % "1.1.0"

//CIRCE JSON Parser, for scala
libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % "0.12.2")

//Lucene
val luceneVersion = "8.2.0"
libraryDependencies ++= Seq(
  "org.apache.lucene" % "lucene-core" % luceneVersion,
  "org.apache.lucene" % "lucene-analyzers-common" % luceneVersion,
  "org.apache.lucene" % "lucene-queryparser" % luceneVersion,
  "org.apache.lucene" % "lucene-facet" % luceneVersion,
  "org.apache.lucene" % "lucene-highlighter" % luceneVersion,
)

//Machine learning
libraryDependencies ++= Seq(
  // Last stable release
  "org.scalanlp" %% "breeze" % "1.0",

  // Native libraries are not included by default. add this if you want them
  // Native libraries greatly improve performance, but increase jar sizes.
  // It also packages various blas implementations, which have licenses that may or may not
  // be compatible with the Apache License. No GPL code, as best I know.
  //"org.scalanlp" %% "breeze-natives" % "1.0",

  // The visualization library is distributed separately as well.
  // It depends on LGPL code
  //"org.scalanlp" %% "breeze-viz" % "1.0"
)
libraryDependencies += "com.github.haifengl" % "smile-core" % "2.0.0"

resolvers += "central" at "http://maven.aliyun.com/nexus/content/groups/public/"
externalResolvers := Resolver.defaults

//native package
enablePlugins(JavaServerAppPackaging)

mainClass in Compile := Some("wiki.dig.http.HttpServer")

//解决windows的line too long问题
scriptClasspath := Seq("*")

//Skip javadoc
mappings in(Compile, packageDoc) := Seq()

//把运行时需要的配置文件拷贝到打包后的主目录下

import NativePackagerHelper._

//mappings in Universal += file("my.conf") -> "my.conf"
mappings in Universal ++= directory("www")
mappings in Universal ++= directory("conf")

initialCommands in console +=
  """
    |
  """.stripMargin
