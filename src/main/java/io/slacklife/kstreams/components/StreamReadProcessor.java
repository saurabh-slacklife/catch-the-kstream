package io.slacklife.kstreams.components;

import io.slacklife.models.User;
import io.slacklife.serde.UserSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class StreamReadProcessor {

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

    KStream<String, User>[] filterBranch = userStream.branch(
        (key, value) -> value.getName().equalsIgnoreCase("saurabh"),
        (key, value) -> true
    );

    filterBranch[0].to("user-saurabh", Produced.with(stringSerde, userSerde));
    filterBranch[1].to("user-non-saurabh", Produced.with(stringSerde, userSerde));

    KTable<String, Long> kTableCount = userStream
        .groupBy((key, value) -> key, Grouped.with(stringSerde, userSerde))
        .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("user-count-agg-store")
            .withKeySerde(stringSerde));

    KStream<String, Long> kCountStream = kTableCount.toStream();

    kCountStream.print(Printed.toSysOut());

    kCountStream.to("user-count-stream", Produced.with(stringSerde, Serdes.Long()));

  }

}
