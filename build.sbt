name := "ImplTest"

version := "0.0.1"

scalaVersion := "2.12.1"

Compile/mainClass := Some("impltest.Main")

lazy val adaptiveCEPProject = RootProject(uri("https://github.com/pweisenburger/AdaptiveCEP.git"))
lazy val secureScalaProject = RootProject(uri("https://github.com/allprojects/securescala.git"))
lazy val root = (project in file(".")).dependsOn(adaptiveCEPProject).dependsOn(secureScalaProject)

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor"   % "2.4.16",
  "com.typesafe.akka" %% "akka-testkit" % "2.4.16"  % "test",
  "com.espertech"     %  "esper"        % "5.5.0",
  "org.scalatest"     %% "scalatest"    % "3.0.1"   % "test"
)
