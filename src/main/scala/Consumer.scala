import java.time.Duration
import java.util

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import java.util.Properties

import scala.collection.JavaConverters._
import java.io._

object Consumer {

  def main(args: Array[String]): Unit = {
    consumeFromKafka("test1")
  }

  def consumeFromKafka(topic: String) = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put("group.id", "consumer-group")
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList(topic))
    while (true) {
      val record = consumer.poll(Duration.ofMillis(1000)).asScala
      record.foreach { record =>
        //println(record.value().toString)                             //AFFICHE LE MESSAGE RECU
        val fw = new FileWriter("src/main/resources/result.txt", true) ;
        fw.write(record.value().toString +"\n") ;
        fw.close()
        if (record.value().toString.split(':')(6) != "Normal" ){       //
          println("Le " + record.value().toString.split(':')(0) + " n'est pas dans un état normal " + record.value().toString.split(':')(6))
        }
        if (record.value().toString.split(':')(7).toInt >= 21600 ){   //6H EN SECONDES
          println("Le " + record.value().toString.split(':')(0) + " doit retourner au centre car le " + record.value().toString.split(':')(1) + " a fini son service")
        }
        if (record.value().toString.split(':')(4).toInt <= 10 && record.value().toString.split(':')(4).toInt > 0  ){       //ERREUR DE CARBURANT
          println("Le " + record.value().toString.split(':')(0) + " doit retourner passer à la pompe à essence, il reste " + record.value().toString.split(':')(4) + "% du réservoir !")
        }

      }
      Thread.sleep(500)
    }
  }
}