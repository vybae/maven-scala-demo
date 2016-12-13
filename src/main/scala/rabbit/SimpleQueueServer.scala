package rabbit

import com.rabbitmq.client._
import java.util.TimerTask
import java.io.IOException
import java.util.concurrent.TimeoutException

class SimpleQueueServer extends RabbitOpBase {
  
  def emit(args: Array[String]) = {
    
    val connection = factory.newConnection()
    val channel = connection.createChannel()
    channel.queueDeclare(SIMPLE_QUEUE_NAME, durable, false, false, null)
    val message = getMessage(args)
    // in this scene, rabbitmq uses default exchange, the received queue's route key is its queue name
    channel.basicPublish("", SIMPLE_QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes())
    println(" [x] Sent '" + message + "'")
    channel.close()
    connection.close()
  }
  
  def receive() {
    channel.queueDeclare(SIMPLE_QUEUE_NAME, durable, false, false, null)
    channel.basicQos(1)
    println(" [*] Waiting for messages. To exit press CTRL+C")

    val consumer: Consumer = new DefaultConsumer(channel) {
      override def handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties,
        body: Array[Byte]) {
        val message = new String(body, "UTF-8")
        println(" [x] Received '" + message + "'")
        try {
          doWork(message)
        } finally {
          channel.basicAck(envelope.getDeliveryTag(), false)
          println(" [x] Done")
        }
      }
    }
    channel.basicConsume(SIMPLE_QUEUE_NAME, autoAck, consumer)

    dispose(180)
  }

  def getMessage(strings: Array[String]): String = if (strings == null || strings.length < 1) "Hello World!" else strings.reduce((a, b) => a + " " + b)
  
  def doWork(msg: String) {
    if (null != msg) Thread.sleep(msg.length() * 300)
  }
}