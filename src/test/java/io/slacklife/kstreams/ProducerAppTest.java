package io.slacklife.kstreams;

import io.slacklife.models.User;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ProducerAppTest {

  static Properties props = new Properties();

  @BeforeAll
  public static void configureProps() {
    props.setProperty("bootstrap.servers", "localhost:9092");
    props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.setProperty("value.serializer", "io.slacklife.serialization.UserSerializer");
    props.setProperty("acks", "1");
  }

  @Test
  public void callProducer() {
    User user = new User();
    user.setId("id-8");
    user.setName("Saurabhqq");

    ProducerRecord<String, User> producerRecord = new ProducerRecord<>("user", user.getId(), user);

    Producer<String, User> producer = new KafkaProducer<>(props);

    Future<RecordMetadata> futureRecordMetadata = producer.send(producerRecord);

    try {
      RecordMetadata recordMetadata = futureRecordMetadata.get();
      assertNotNull(recordMetadata.offset());
      assertNotNull(recordMetadata.timestamp());
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    } finally {
      producer.close();

    }

  }

}
