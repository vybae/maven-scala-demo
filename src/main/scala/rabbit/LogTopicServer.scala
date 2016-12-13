package rabbit

import org.apache.commons.lang3.StringUtils
import java.util.Scanner
import com.rabbitmq.client._

class LogTopicServer extends RabbitOpBase {

  def emit() {

    channel.exchangeDeclare(TOPIC_EXCHANGE_NAME, BuiltinExchangeType.TOPIC)
    // unroutable message listener
    // when it publishes a mandatory message to exchange
    channel.addReturnListener(new ReturnListener() {
      override def handleReturn(replyCode: Int, replyText: String, exchange: String, routingKey: String,
        properties: AMQP.BasicProperties, body: Array[Byte]) {
        println("replyCode: " + replyCode + ", replyText: " + replyText + ", exchange: " + exchange
          + ", routingKey: " + routingKey + ", msg: " + new String(body))
      }
    })

    channel.addShutdownListener(new ShutdownListener() {
      override def shutdownCompleted(cause: ShutdownSignalException) {
        
      }
    })

    val sc = new Scanner(System.in)
    println(" [*] Send messages to consumers. To exit enter quit")
    var message: String = sc.nextLine()
    while (message != "quit") {
      println(" [*] Please enter route key.")
      val routeKey = sc.nextLine()
      // the third param is mandatory, when we send a unroutable message to rmq, the broker will return message to producer
      channel.basicPublish(TOPIC_EXCHANGE_NAME, if (StringUtils.isNotBlank(routeKey)) routeKey else "", true, null, message.getBytes());
      println(" [x] Sent '" + message + "'");
      println(" [*] Send messages to consumers. To exit enter quit")
      message = sc.nextLine()
    }

    sc.close()
    dispose(180)
  }

  def receive() {

    channel.exchangeDeclare(TOPIC_EXCHANGE_NAME, BuiltinExchangeType.TOPIC)
    val queueName = channel.queueDeclare().getQueue()
    val sc = new java.util.Scanner(System.in)
    println(" [*] Please enter exchange's route rule")
    val routeRule = sc.nextLine()

    if (org.apache.commons.lang3.StringUtils.isNotBlank(routeRule)) {
      for (rule <- routeRule.split(":")) {
        channel.queueBind(queueName, TOPIC_EXCHANGE_NAME, rule.trim)
      }
      println(" [*] Waiting for messages. To exit press CTRL+C")

      val consumer: Consumer = new DefaultConsumer(channel) {
        override def handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties,
          body: Array[Byte]) {
          val message = new String(body, "UTF-8")
          System.out.println(" [x] " + routeRule + " Received '" + envelope.getRoutingKey() + "':'" + message + "'");
        }
      }
      channel.basicConsume(queueName, autoAck, consumer)

      dispose(180)
    }
  }
}