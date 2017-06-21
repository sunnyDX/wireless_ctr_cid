package util

import com.rabbitmq.client.ConnectionFactory

/**
  * Created by dengxing on 2017/5/23.
  */
object MQSender {

  val connFac = new ConnectionFactory()
  connFac.setHost("192.168.5.63")
  connFac.setPort(5672)

  val conn = connFac.newConnection()
  val channel = conn.createChannel()

  val queueName: String = "kaleidoscope"

  def sendToMQ(user_tag: String): Unit = {

    //channel.queueDeclare(queueName, true , false, false, null)
    channel.basicPublish("", queueName, null, user_tag.getBytes())
    // print("send message[" + user_tag + "] to " + queueName + " success!")
  }

  def close(): Unit = {
    channel.close()
    conn.close()
  }
}


