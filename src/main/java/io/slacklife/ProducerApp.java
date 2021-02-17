package io.slacklife;

import io.slacklife.models.User;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerApp {

  public static void main(String[] args) {
    Properties props = new Properties();
    props.setProperty("bootstrap.servers", "localhost:9092");
    props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.setProperty("value.serializer", "io.slacklife.serialization.UserSerializer");
    props.setProperty("acks", "1");

    User user = new User();
    user.setId("id-3:1");
    user.setName("Saurabh");

    ProducerRecord<String, User> producerRecord = new ProducerRecord<>("user", user.getId(), user);

    Producer<String, User> producer = new KafkaProducer<>(props);

    producer.send(producerRecord);
    producer.close();
  }
}
