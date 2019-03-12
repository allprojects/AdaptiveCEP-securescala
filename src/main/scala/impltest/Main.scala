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

  // --- A < B

  val lessQuery: Query2[EncInt, EncInt] = 
    stream[EncInt]("A")
    .join(stream[EncInt]("B"), slidingWindow(2.seconds), slidingWindow(2.seconds))
    .where((a, b) => interpret(a < b))

  val lessActorSystem: ActorSystem = ActorSystem()
  val lessPublishers: Map[String, ActorRef] = Map(
    "A" -> lessActorSystem.actorOf(Props(RandomPublisher(id => Event1( Common.encrypt(Comparable, keyRing)( BigInt(id) ) ))), "A"),
    "B" -> lessActorSystem.actorOf(Props(RandomPublisher(id => Event1( Common.encrypt(Comparable, keyRing)( BigInt(id * 2) ) ))), "B")
  )
  val lessGraph: ActorRef = GraphFactory.create(
    actorSystem = lessActorSystem, query = lessQuery, publishers = lessPublishers,
    frequencyMonitorFactory = AverageFrequencyMonitorFactory (interval = 15, logging = true),
    latencyMonitorFactory = PathLatencyMonitorFactory (interval =  5, logging = true),
    createdCallback = () => println("STATUS:\tLess graph has been created."))(
    eventCallback = {
      case (i1, i2) => {
        val decI1 = Common.decrypt(keyRing.priv)(i1)
        val decI2 = Common.decrypt(keyRing.priv)(i2)
        println(s"A < B:\t ( $decI1, $decI2 )")
      }
      case _                             =>
    })

  // --- A < B

  // --- Sum is even

  val sumEvenQuery: Query2[EncInt, EncInt] = 
    stream[EncInt]("C")
    .join(stream[EncInt]("D"), slidingWindow(2.seconds), slidingWindow(2.seconds))
    .where(
      (a, b) => interpret(
        isEven(
          interpret(a + b)
        )
      )
    )

  val sumEvenActorSystem: ActorSystem = ActorSystem()
  val sumEvenPublishers: Map[String, ActorRef] = Map(
    "C" -> sumEvenActorSystem.actorOf(Props(RandomPublisher(id => Event1( Common.encrypt(Comparable, keyRing)( BigInt(id) ) ))), "C"),
    "D" -> sumEvenActorSystem.actorOf(Props(RandomPublisher(id => Event1( Common.encrypt(Comparable, keyRing)( BigInt(id * 2) ) ))), "D")
  )
  val sumEvenGraph: ActorRef = GraphFactory.create(
    actorSystem = sumEvenActorSystem, query = sumEvenQuery, publishers = sumEvenPublishers,
    frequencyMonitorFactory = AverageFrequencyMonitorFactory (interval = 15, logging = true),
    latencyMonitorFactory = PathLatencyMonitorFactory (interval =  5, logging = true),
    createdCallback = () => println("STATUS:\tSum even graph has been created."))(
    eventCallback = {
      case (i1, i2) => {
        val decI1 = Common.decrypt(keyRing.priv)(i1)
        val decI2 = Common.decrypt(keyRing.priv)(i2)
        println(s"C + D is even:\t ( $decI1, $decI2 )")
      }
      case _                             =>
    })

  // --- Sum is even

  // --- Concat strings

  val concatQuery: Query2[EncString, EncString] = 
    stream[EncString]("E")
    .join(stream[EncString]("F"), slidingWindow(2.seconds), slidingWindow(2.seconds))
    .where(
      (a, b) => interpret(
        equalStr(
          interpret(
            concatStr(a, b)
          ),
          interpret(
            concatStr(b, a)
          )
        )
      )
    )

  val concatActorSystem: ActorSystem = ActorSystem()
  val concatPublishers: Map[String, ActorRef] = Map(
    "E" -> concatActorSystem.actorOf(Props(RandomPublisher(id => Event1( Common.encryptStrOpe(keyRing)(s"$id") ))), "E"),
    "F" -> concatActorSystem.actorOf(Props(RandomPublisher(id => Event1( Common.encryptStrOpe(keyRing)(s"$id") ))), "F")
  )
  val concatGraph: ActorRef = GraphFactory.create(
    actorSystem = concatActorSystem, query = concatQuery, publishers = concatPublishers,
    frequencyMonitorFactory = AverageFrequencyMonitorFactory (interval = 15, logging = true),
    latencyMonitorFactory = PathLatencyMonitorFactory (interval =  5, logging = true),
    createdCallback = () => println("STATUS:\tString graph has been created."))(
    eventCallback = {
      case (s1, s2) => {
        val decS1 = Common.decryptStr(keyRing.priv)(s1)
        val decS2 = Common.decryptStr(keyRing.priv)(s2)
        println(s"EF is equal to FE:\t ( $decS1$decS2 == $decS2$decS1 )")
      }
      case _                             =>
    })

  // --- Concat strings

}
