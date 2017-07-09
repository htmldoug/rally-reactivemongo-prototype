name := "reactive-prototype"

version := "1.0"

scalaVersion := "2.12.2"

enablePlugins(JmhPlugin)

libraryDependencies += "org.reactivemongo" %% "reactivemongo" % "0.12.5"

libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "2.1.0"

libraryDependencies += "joda-time" % "joda-time" % "2.9.9"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

libraryDependencies += "org.tinygroup" % "org.tinygroup.slf4j-log4j12" % "1.7.7"
