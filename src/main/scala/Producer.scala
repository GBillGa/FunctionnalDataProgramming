import java.util.Properties

import org.apache.kafka.clients.producer._

import scala.io.Source


object Producer {

  def main(args: Array[String]): Unit = {
    writeToKafka("test1")
  }

  def writeToKafka(topic: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val busData = "Bus1:Chauffeur1:20:15:22:3000:Normal:21699"
    produceMessage("test1",producer,readFromFile("src/main/resources/init.txt"),0)
    producer.close()

  }

  def readFromFile(fileName: String): Array[String] ={
    val buffer = Source.fromFile(fileName)
    val lines = buffer.getLines.toArray
    return lines
  }

  def produceMessage(topicName: String,producer: KafkaProducer[String, String],messages:Array[String],messageToRead: Int): Unit= {
    val msgNumber = messageToRead
    val record = new ProducerRecord[String, String](topicName, "key", messages(msgNumber))
    println("|" + record.value().toString + "|" + record.toString)
    producer.send(record)
    Thread.sleep(100)
    if (messageToRead < messages.size - 1){
      produceMessage(topicName,producer,messages,msgNumber+1)
    }
  }
}

