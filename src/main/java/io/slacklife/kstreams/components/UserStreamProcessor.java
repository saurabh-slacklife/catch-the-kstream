package io.slacklife.kstreams.components;

import io.slacklife.models.User;
import io.slacklife.serde.UserSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.QueryableStoreTypes.KeyValueStoreType;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.QueryableStoreProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class UserStreamProcessor {

  @Autowired
  ApplicationContext applicationContext;

  @Autowired
  public void process(final StreamsBuilder streamsBuilder) {
    Serde<String> stringSerde = Serdes.String();
    Serde<User> userSerde = UserSerdes.User();

    KStream<String, User> userStream = streamsBuilder
        .stream("user", Consumed.with(stringSerde, userSerde));

    userStream.process(() -> new AbstractProcessor<>() {
      @Override
      public void process(String key, User value) {
        log.info("Offset: {} for topic: {} and key: {} at partition: {}",
            this.context().offset(), this.context().topic(),
            key, this.context().topic());
      }
    });

//    KStream<String, User>[] filterBranch = userStream.branch(
//        (key, value) -> value.getName().equalsIgnoreCase("saurabh"),
//        (key, value) -> true
//    );
//
//    filterBranch[0].to("user-saurabh", Produced.with(stringSerde, userSerde));
//    filterBranch[1].to("user-non-saurabh", Produced.with(stringSerde, userSerde));

    userStream.groupBy((key, value) -> key, Grouped.with(stringSerde, userSerde))
        .count(
            Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("user-count-state-store")
                .withCachingEnabled());

//    StreamsBuilderFactoryBean streamsBuilderFactoryBean = applicationContext
//        .getBean("StreamReadProcessor", StreamsBuilderFactoryBean.class);
//    KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
//    StoreQueryParameters<ReadOnlyKeyValueStore<String, Long>> storeQueryParameters = StoreQueryParameters
//        .fromNameAndType("user-count-state-store", QueryableStoreTypes.keyValueStore());
//
//    ReadOnlyKeyValueStore<String, Long> keyValueStore = kafkaStreams.store(storeQueryParameters);
//    System.out.println(keyValueStore.get("Saurabh"));

  }

}
