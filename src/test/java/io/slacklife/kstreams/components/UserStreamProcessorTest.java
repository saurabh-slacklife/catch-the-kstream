package io.slacklife.kstreams.components;

import static org.junit.jupiter.api.Assertions.*;

import io.slacklife.models.User;
import io.slacklife.serde.UserSerdes;
import io.slacklife.serialization.UserSerializer;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class UserStreamProcessorTest {

  static Properties properties = new Properties();

  StreamsBuilder streamsBuilder;

  @BeforeAll
  static void setUp() {

    properties.putAll(Map.of(StreamsConfig.APPLICATION_ID_CONFIG, "kstream-word-count",
        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"));

  }

  @BeforeEach
  void setupBeforeMethod() {
    this.streamsBuilder = new StreamsBuilder();
  }


  @Test
  public void testProcess() {

    UserStreamProcessor userStreamProcessor = new UserStreamProcessor();
    userStreamProcessor.process(streamsBuilder);

    try (TopologyTestDriver topologyTestDriver = new TopologyTestDriver(streamsBuilder.build(),
        properties)) {
      User user = new User();
      user.setId("id-8");
      user.setName("Saurabhqq");

      TestInputTopic<String, User> userTopicInput = topologyTestDriver
          .createInputTopic("user", Serdes.String().serializer(), UserSerdes.User()
              .serializer());

      userTopicInput.pipeInput("id-1",user);
      userTopicInput.pipeInput("id-2",user);
      userTopicInput.pipeInput("id-3",user);
      userTopicInput.pipeInput("id-4",user);
      userTopicInput.pipeInput("id-1",user);
      userTopicInput.pipeInput("id-1",user);
      userTopicInput.pipeInput("id-1",user);

      KeyValueStore<String, Long> keyValueStore = topologyTestDriver
          .getKeyValueStore("user-count-state-store");

      Assertions.assertThat(keyValueStore.get("id-1")).isEqualTo(4);
      Assertions.assertThat(keyValueStore.get("id-2")).isEqualTo(1);
      Assertions.assertThat(keyValueStore.get("id-3")).isEqualTo(1);

      KeyValueIterator<String, Long> all = keyValueStore.all();
      while(all.hasNext()){
        KeyValue<String, Long> next = all.next();
        System.out.println(next.key+":"+next.value);

      }

    }

  }


}