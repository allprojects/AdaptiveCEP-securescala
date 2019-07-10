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

  //1 X + Y + Z is even

  val threeIsEvenQuery: Query3[EncInt, EncInt, EncInt] = 
    stream[EncInt]("X")
    .join(stream[EncInt]("Y"), slidingWindow(2.seconds), slidingWindow(2.seconds))
    .join(stream[EncInt]("Z"), slidingWindow(3.seconds), slidingWindow(3.seconds))
    .where((a , b , c) => interpret(isEven(interpret(interpret(a+b)+c))))

  val threeIsEvenActorSystem: ActorSystem = ActorSystem()
  val threeIsEvenPublishers: Map[String, ActorRef] = Map(
    "X" -> threeIsEvenActorSystem.actorOf(Props(RandomPublisher(id => Event1( Common.encrypt(Comparable, keyRing)( BigInt(id) ) ))), "X"),
    "Y" -> threeIsEvenActorSystem.actorOf(Props(RandomPublisher(id => Event1( Common.encrypt(Comparable, keyRing)( BigInt(id * 2) ) ))), "Y"),
    "Z" -> threeIsEvenActorSystem.actorOf(Props(RandomPublisher(id => Event1( Common.encrypt(Comparable, keyRing)( BigInt(id * 3) ) ))), "Z")
  )
  val threeIsEvenGraph: ActorRef = GraphFactory.create(
    actorSystem = threeIsEvenActorSystem, query = threeIsEvenQuery, publishers = threeIsEvenPublishers,
    frequencyMonitorFactory = AverageFrequencyMonitorFactory (interval = 15, logging = true),
    latencyMonitorFactory = PathLatencyMonitorFactory (interval =  5, logging = true),
    createdCallback = () => println("STATUS:\tsum three is even graph has been created."))(
    eventCallback = {
      case (i1, i2, i3) => {
		val decI1 = Common.decrypt(keyRing.priv)(i1)
		val decI2 = Common.decrypt(keyRing.priv)(i2)
		val decI3 = Common.decrypt(keyRing.priv)(i3)
		println(s"X + Y + Z is even:\t ( $decI1, $decI2, $decI3)")
      }
      case _                             =>
    })

//2  G + H > I

  val moreQuery: Query3[EncInt, EncInt, EncInt] = 
    stream[EncInt]("G")
    .join(stream[EncInt]("H"),slidingWindow(2.seconds),slidingWindow(2.seconds))
    .join(stream[EncInt]("I"),slidingWindow(2.seconds),slidingWindow(2.seconds))
    .where((a, b, c) => interpret( interpret(a + b) > c))

  val moreActorSystem: ActorSystem = ActorSystem()
  val morePublishers: Map[String, ActorRef] = Map(
    "G" -> moreActorSystem.actorOf(Props(RandomPublisher(id => Event1( Common.encrypt(Comparable, keyRing)( BigInt(id) ) ))), "G"),
    "H" -> moreActorSystem.actorOf(Props(RandomPublisher(id => Event1( Common.encrypt(Comparable, keyRing)( BigInt(id * 2) ) ))), "H"),
    "I" -> moreActorSystem.actorOf(Props(RandomPublisher(id => Event1( Common.encrypt(Comparable, keyRing)( BigInt(id * 2) ) ))), "I")
  )
  val moreGraph: ActorRef = GraphFactory.create(
    actorSystem = moreActorSystem, query = moreQuery, publishers = morePublishers,
    frequencyMonitorFactory = AverageFrequencyMonitorFactory (interval = 15, logging = true),
    latencyMonitorFactory = PathLatencyMonitorFactory (interval =  5, logging = true),
    createdCallback = () => println("STATUS:\tmore graph has been created."))(
    eventCallback = {
      case (i1, i2, i3) => {
        val decI1 = Common.decrypt(keyRing.priv)(i1)
        val decI2 = Common.decrypt(keyRing.priv)(i2)
	val decI3 = Common.decrypt(keyRing.priv)(i3)
        println(s" G + H > I:\t ( $decI1, $decI2, $decI3 )")
      }
      case _                             =>
    })

//3   K * M > J * L 

  val multQuery: Query4[EncInt, EncInt, EncInt, EncInt] = 
    stream[EncInt]("J")
    .join(stream[EncInt]("K"),slidingWindow(2.seconds),slidingWindow(2.seconds))
    .join(stream[EncInt]("L"),slidingWindow(2.seconds),slidingWindow(2.seconds))
    .join(stream[EncInt]("M"),slidingWindow(2.seconds),slidingWindow(2.seconds))
    .where((a, b, c, d) => interpret(interpret(b * d) > interpret(a * c)))

  val multActorSystem: ActorSystem = ActorSystem()
  val multPublishers: Map[String, ActorRef] = Map(
    "J" -> multActorSystem.actorOf(Props(RandomPublisher(id => Event1( Common.encrypt(Comparable, keyRing)( BigInt(id) ) ))), "J"),
    "K" -> multActorSystem.actorOf(Props(RandomPublisher(id => Event1( Common.encrypt(Comparable, keyRing)( BigInt(id * 2) ) ))), "K"),
    "L" -> multActorSystem.actorOf(Props(RandomPublisher(id => Event1( Common.encrypt(Comparable, keyRing)( BigInt(id * 2) ) ))), "L"),
    "M" -> multActorSystem.actorOf(Props(RandomPublisher(id => Event1( Common.encrypt(Comparable, keyRing)( BigInt(id * 3) ) ))), "M")
  )
  val multGraph: ActorRef = GraphFactory.create(
    actorSystem = multActorSystem, query = multQuery, publishers = multPublishers,
    frequencyMonitorFactory = AverageFrequencyMonitorFactory (interval = 15, logging = true),
    latencyMonitorFactory = PathLatencyMonitorFactory (interval =  5, logging = true),
    createdCallback = () => println("STATUS:\tmult graph has been created."))(
    eventCallback = {
      case (i1, i2, i3, i4) => {
        val decI1 = Common.decrypt(keyRing.priv)(i1)
        val decI2 = Common.decrypt(keyRing.priv)(i2)
	val decI3 = Common.decrypt(keyRing.priv)(i3)
	val decI4 = Common.decrypt(keyRing.priv)(i4)
        println(s" K * M > J * L:\t ( $decI2, $decI4, $decI1, $decI3 )")
      }
      case _                             =>
    })

//4 NO equals to OP

  val concat2Query: Query3[EncString, EncString, EncString] = 
    stream[EncString]("N")
    .join(stream[EncString]("O"), slidingWindow(2.seconds), slidingWindow(2.seconds))
    .join(stream[EncString]("P"), slidingWindow(2.seconds), slidingWindow(2.seconds))
    .where(
      (a, b, c) => interpret(
        equalStr(
          interpret(
            concatStr(a, b)
          ),
          interpret(
            concatStr(b, c)
          )
        )
      )
    )

  val concat2ActorSystem: ActorSystem = ActorSystem()
  val concat2Publishers: Map[String, ActorRef] = Map(
    "N" -> concat2ActorSystem.actorOf(Props(RandomPublisher(id => Event1( Common.encryptStrOpe(keyRing)(s"$id") ))), "E"),
    "O" -> concat2ActorSystem.actorOf(Props(RandomPublisher(id => Event1( Common.encryptStrOpe(keyRing)(s"$id") ))), "F"),
    "P" -> concat2ActorSystem.actorOf(Props(RandomPublisher(id => Event1( Common.encryptStrOpe(keyRing)(s"$id") ))), "P")
  )
  val concat2Graph: ActorRef = GraphFactory.create(
    actorSystem = concat2ActorSystem, query = concat2Query, publishers = concat2Publishers,
    frequencyMonitorFactory = AverageFrequencyMonitorFactory (interval = 15, logging = true),
    latencyMonitorFactory = PathLatencyMonitorFactory (interval =  5, logging = true),
    createdCallback = () => println("STATUS:\tstring 2 graph has been created."))(
    eventCallback = {
      case (s1, s2, s3) => {
        val decS1 = Common.decryptStr(keyRing.priv)(s1)
        val decS2 = Common.decryptStr(keyRing.priv)(s2)
	val decS3 = Common.decryptStr(keyRing.priv)(s3)
        println(s"NO is equal to OP:\t ( $decS1$decS2 == $decS2$decS3 )")
      }
      case _                             =>
    })
}
