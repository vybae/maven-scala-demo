package rabbit

import java.io.IOException
import java.util.TimerTask
import com.rabbitmq.client._
import java.util.concurrent.TimeoutException
import java.util.concurrent.Executors

class RabbitOpBase {
  protected val autoAck = true
  protected val durable = true
  
  protected val SIMPLE_QUEUE_NAME = "hello"
  protected val RPC_QUEUE_NAME = "fib_rpc_queue"
  
  protected val SIMPLE_EXCHANGE_NAME = "hellow"
  protected val DIRECT_EXCHANGE_NAME = "hello_direct_ex"
  protected val TOPIC_EXCHANGE_NAME = "hello_topic_ex"
  
  protected val factory = new ConnectionFactory()
//  factory setHost "192.168.197.200"
//  factory setUsername "admin"
//  factory setPassword "123"
  factory setUri "amqp://admin:123@192.168.197.200:5672/%2f"
  protected val connection = factory.newConnection()
  protected val channel = connection.createChannel()
  
  def dispose(sec: Int) {
    val timer = new java.util.Timer()
    timer.schedule(new TimerTask() {
      override def run() {
        try {
          if (channel != null) channel.close()
          if (connection != null) connection.close()
        } catch {
          case e: IOException => e.printStackTrace()
          case e: TimeoutException => e.printStackTrace()
        }
        timer.cancel()
        println("Consumer exit")
      }
    }, sec * 1000)
  }
  
  def test() {
    val es = Executors.newFixedThreadPool(10)
    factory.newConnection(es)
  }
}