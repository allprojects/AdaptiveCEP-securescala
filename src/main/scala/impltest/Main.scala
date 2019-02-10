package impltest

import akka.actor.{ActorRef, ActorSystem, Props}
import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.dsl.Dsl._
import adaptivecep.graph.factory._
import adaptivecep.graph.qos._
import adaptivecep.publishers._

import crypto._
import crypto.cipher._
import crypto.dsl._
import crypto.dsl.Implicits._

object Main extends App {

  println("\n--------------------------------------------------")
  println("Test project combining AdaptiveCEP and SecureScala")
  println("--------------------------------------------------\n")

  lazy val keyRing = KeyRing.create
  val interpret = new LocalInterpreter(keyRing)
  def isSmaller(a: EncInt, b: EncInt): Boolean = interpret(a < b)

  val actorSystem: ActorSystem = ActorSystem()

  val publisherA: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1( Common.encrypt(Comparable, keyRing)( BigInt(id) ) ))),             "A")
  val publisherB: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1( Common.encrypt(Comparable, keyRing)( BigInt(id * 2) ) ))),         "B")
  val publisherC: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1( Common.encrypt(Comparable, keyRing)( BigInt(id * 3) ) ))),     "C")
  val publisherD: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1( Common.encryptStrOpe(keyRing)(s"String($id)") ))), "D")

  val publishers: Map[String, ActorRef] = Map(
    "A" -> publisherA,
    "B" -> publisherB,
    "C" -> publisherC,
    "D" -> publisherD)

  val query1: Query3[Either[EncInt, EncString], Either[EncInt, X], Either[EncInt, X]] =
    stream[EncInt]("A")
    .join(
      stream[EncInt]("B"),
      slidingWindow(2.seconds),
      slidingWindow(2.seconds))
    .where(isSmaller(_, _))
    .dropElem1(
      latency < timespan(1.milliseconds) otherwise { (nodeData) => println(s"PROBLEM:\tEvents reach node `${nodeData.name}` too slowly!") })
    .selfJoin(
      tumblingWindow(1.instances),
      tumblingWindow(1.instances),
      frequency > ratio( 3.instances,  5.seconds) otherwise { (nodeData) => println(s"PROBLEM:\tNode `${nodeData.name}` emits too few events!") },
      frequency < ratio(12.instances, 15.seconds) otherwise { (nodeData) => println(s"PROBLEM:\tNode `${nodeData.name}` emits too many events!") })
    .and(stream[EncInt]("C"))
    .or(stream[EncString]("D"))

  val graph: ActorRef = GraphFactory.create(
    actorSystem =             actorSystem,
    query =                   query1,
    publishers =              publishers,
    frequencyMonitorFactory = AverageFrequencyMonitorFactory  (interval = 15, logging = true),
    latencyMonitorFactory =   PathLatencyMonitorFactory       (interval =  5, logging = true),
    createdCallback =         () => println("STATUS:\t\tGraph has been created."))(
    eventCallback =           {
      case (Left(i1), Left(i2), Left(f)) => println(s"COMPLEX EVENT:\tEvent3($i1,$i2,$f)")
      case (Right(s), _, _)              => println(s"COMPLEX EVENT:\tEvent1($s)")
      case _                             =>
    })

}
