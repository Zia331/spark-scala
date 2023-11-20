ThisBuild / scalaVersion     := "2.12.18"
ThisBuild / version          := "1.0"

libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.8"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.8"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.8"
libraryDependencies += "org.apache.commons" % "commons-csv" % "1.8"
