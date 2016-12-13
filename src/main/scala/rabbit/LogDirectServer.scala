package rabbit

import com.rabbitmq.client._
import java.util.Scanner
import org.apache.commons.lang3.StringUtils

class LogDirectServer extends RabbitOpBase {
  
  def emit() {

    channel.exchangeDeclare(DIRECT_EXCHANGE_NAME, BuiltinExchangeType.DIRECT)
    val sc = new Scanner(System.in)

    println(" [*] Send messages to consumers. To exit enter quit")
    var message: String = sc.nextLine()
    while (message != "quit") {
      println(" [*] Please enter route key.")
      val routeKey = sc.nextLine()
      channel.basicPublish(DIRECT_EXCHANGE_NAME, if (StringUtils.isNotBlank(routeKey)) routeKey else "", null, message.getBytes());
      println(" [x] Sent '" + message + "'");
      println(" [*] Send messages to consumers. To exit enter quit")
      message = sc.nextLine()
    }

    sc.close()
    dispose(180)
  }
  
  def receive() {
    channel.exchangeDeclare(DIRECT_EXCHANGE_NAME, BuiltinExchangeType.DIRECT)
    val queueName = channel.queueDeclare().getQueue()
    channel.queueBind(queueName, DIRECT_EXCHANGE_NAME, "foo")
    channel.queueBind(queueName, DIRECT_EXCHANGE_NAME, "black")
    
    println(" [*] Waiting for messages. To exit press CTRL+C")
    
    val consumer: Consumer = new DefaultConsumer(channel) {
      override def handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties,
        body: Array[Byte]) {
        val message = new String(body, "UTF-8")
        System.out.println(" [x] Received '" + envelope.getRoutingKey() + "':'" + message + "'");
      }
    }
    channel.basicConsume(queueName, autoAck, consumer)

    dispose(180)
  }
}