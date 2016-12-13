package rabbit

import com.rabbitmq.client._

class FibonacciRPCServer extends RabbitOpBase {
  
  def emit() {

    val corrId = java.util.UUID.randomUUID().toString()
    val replyQueueName = channel.queueDeclare().getQueue()
    val consumer = new DefaultConsumer(channel) {
      override def handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties,
        body: Array[Byte]) {
        if (properties.getCorrelationId.equals(corrId)) {
          println(" [.] Got Fibonacci '" + new String(body) + "'")
        }
      }
    }
    channel.basicConsume(replyQueueName, true, consumer)

    val props = new AMQP.BasicProperties.Builder()
      .correlationId(corrId)
      .replyTo(replyQueueName)
      .build()
    val sc = new java.util.Scanner(System.in)
    println("Please enter a nature number")
    var message = sc.nextLine()
    while (org.apache.commons.lang3.StringUtils.isNotBlank(message)) {
      channel.basicPublish("", RPC_QUEUE_NAME, props, message.getBytes())
      println("Please enter a nature number")
      message = sc.nextLine()
    }

    sc.close()
    dispose(180)
  }

  def receive() {
    channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null)
    channel.basicQos(1)

    val consumer = new DefaultConsumer(channel) {
      override def handleDelivery(consumerTag: String, envelope: Envelope, properties: AMQP.BasicProperties,
        body: Array[Byte]) {
        val replyProps = new AMQP.BasicProperties.Builder()
          .correlationId(properties.getCorrelationId())
          .build()
        val message = new String(body)
        println(" [.] fibonacci(" + message + ")")
        val n = Integer.parseInt(message)
        
        channel.basicPublish("", properties.getReplyTo(), replyProps, fibonacci(n).toString().getBytes())

        channel.basicAck(envelope.getDeliveryTag(), false)
      }
    }
    channel.basicConsume(RPC_QUEUE_NAME, false, consumer)

    println(" [x] Awaiting RPC requests")
  }

  def fibonacci(n: Int): Int = if (n < 3) 1 else fibonacci(n - 1) + fibonacci(n - 2)
}