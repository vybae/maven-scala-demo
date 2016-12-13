package rabbit

import com.rabbitmq.client._
import java.util.regex.Pattern
import java.util.Scanner

class SimpleExchangeServer extends RabbitOpBase {

  def emit() {
    channel.exchangeDeclare(SIMPLE_EXCHANGE_NAME, BuiltinExchangeType.FANOUT)
    val sc = new Scanner(System.in)

    println(" [*] Send messages to consumers. To exit enter quit")
    var message: String = sc.nextLine()
    while (message != "quit") {
      channel.basicPublish(SIMPLE_EXCHANGE_NAME, "", null, message.getBytes());
      println(" [x] Sent '" + message + "'");
      message = sc.nextLine()
    }

    sc.close()
    channel.close()
    connection.close()
  }
  
  def receive() {
    channel.exchangeDeclare(SIMPLE_EXCHANGE_NAME, BuiltinExchangeType.FANOUT)
    val queueName = channel.queueDeclare().getQueue()
    channel.queueBind(queueName, SIMPLE_EXCHANGE_NAME, "")
    
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
    channel.basicConsume(queueName, autoAck, consumer)

    dispose(180)
  }
  
  def doWork(msg: String) {
    if (null != msg) Thread.sleep(msg.length() * 300)
  }
}