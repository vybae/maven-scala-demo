package kafkatest

import akka.actor._
import org.apache.kafka.clients.producer._
import org.apache.kafka.clients.consumer._
import common.CustomPropertyUtil
import java.util.Date

class KafkaSimpleActor(val topic: String, msgNum: Int = 100) extends Actor {

  val producerActor = context.actorOf(Props(new Actor() {
    override def receive = {
      case "produce" => {
        val props = CustomPropertyUtil.load("kafka-producer.properties")

        val producer = new KafkaProducer[String, String](props);
        for (i <- 0 until msgNum) {
          producer.send(new ProducerRecord[String, String](topic, Integer.toString(i), new Date().toString));
          if (i % 100 == 0) {
            producer.flush()
          }
          println(i)
        }

        producer.close();
      }
    }
  }), "producer")
  
  val consumerActor = context.actorOf(Props(new Actor() {
    override def receive = {
      case "consume" => {
        import collection.JavaConversions._

        val props = CustomPropertyUtil.load("kafka-consumer.properties")
        val consumer = new KafkaConsumer[String, String](props);

        consumer.subscribe(List(topic));
        while (true) {
          val records = consumer.poll(100);
          records.foreach(record => printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value()))
        }
      }
    }
  }), "consumer")
  
  override def receive = {
    case "begin" => {
      consumerActor ! "consume"
      producerActor ! "produce"
    }
    case "shutdown" => context.stop(self)
  }
}