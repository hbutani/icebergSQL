scalaVersion in ThisBuild := "2.11.8"

crossScalaVersions := Seq("2.10.5", "2.11.8")

parallelExecution in Test := false

val sparkVersion = "2.4.4"
val icebergVersion = "0.6.3"
val scalatestVersion = "3.0.5"
val icebergSQLVersion = "0.0.1-SNAPSHOT"
val derbyVersion = "10.11.1.1"

resolvers += "jitpack" at "https://jitpack.io"

val coreDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.apache.derby" % "derby" % derbyVersion %   "provided" force(),
  "com.github.netflix.iceberg" % "iceberg-api" % icebergVersion ,
  "com.github.netflix.iceberg" % "iceberg-common" % icebergVersion ,
  "com.github.netflix.iceberg" % "iceberg-core" % icebergVersion 
)

val coreTestDependencies = Seq(
  "org.scalatest" %% "scalatest" % scalatestVersion % "test",
  "org.pegdown"    %  "pegdown"     % "1.6.0"  % "test"
)

lazy val commonSettings = Seq(
  organization := "org.rhbutani",

  version := icebergSQLVersion,

  javaOptions := Seq("-Xms1g", "-Xmx3g",
    "-Duser.timezone=UTC",
    "-Dscalac.patmat.analysisBudget=512",
    "-XX:MaxPermSize=256M"),

  // Target Java 8
  scalacOptions += "-target:jvm-1.8",
  javacOptions in compile ++= Seq("-source", "1.8", "-target", "1.8"),

  scalacOptions := Seq("-feature", "-deprecation"
    //, "-Ylog-classpath"
    ),

  licenses := Seq("Apache License, Version 2.0" ->
    url("http://www.apache.org/licenses/LICENSE-2.0")
  ),

  homepage := Some(url("https://github.com/hbutani/icebergSparkSQL")),

  publishArtifact in (Test,packageDoc) := false,

  pomIncludeRepository := { _ => false },

  test in assembly := {},

  pomExtra := (
    <scm>
      <url>https://github.com/hbutani/icebergSparkSQL.git</url>
      <connection>scm:git:git@github.com:hbutani/icebergSparkSQL.git</connection>
    </scm>
      <developers>
        <developer>
          <name>Harish Butani</name>
          <organization>hbutani</organization>
          <organizationUrl>http://github.com/hbutani</organizationUrl>
        </developer>
      </developers>),

  fork in Test := false,
  parallelExecution in Test := false
)

lazy val root = project.in(file("."))
//    .enablePlugins(GitVersioning)
  .settings(commonSettings: _*)
  .settings(
    name := s"icebergSparkSQL",
    libraryDependencies ++= (coreDependencies ++ coreTestDependencies),
    publishArtifact in (Compile, packageBin) := false,
    publishArtifact in Test := false,
    testOptions in Test ++= Seq(
      Tests.Argument(TestFrameworks.ScalaTest, "-h", "target/test-reports"),
      Tests.Argument(TestFrameworks.ScalaTest, "-oDT")
    ),
    aggregate in assembly := false
  )
  .settings(addArtifact(artifact in (Compile, assembly), assembly).settings: _*)

