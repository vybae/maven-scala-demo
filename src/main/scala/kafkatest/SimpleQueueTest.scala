package kafkatest

import akka.actor._
import java.util.Scanner

object SimpleQueueTest {
  val TOPIC_NAME = "test"

  def main(args: Array[String]): Unit = {
    val system = ActorSystem("kafka-simple-actor")
    val ka = system.actorOf(Props(new KafkaSimpleActor("test")), "kafkasimple")
    
    ka ! Kill
    Predef println (system.actorSelection("/*/kafkasimple/producer") ! "produce")
    system.terminate()
  }
}