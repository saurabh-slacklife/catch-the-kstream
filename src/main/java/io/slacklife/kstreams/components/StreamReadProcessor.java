package io.slacklife.kstreams.components;

import io.slacklife.models.User;
import io.slacklife.serde.UserSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class StreamReadProcessor {

  @Autowired
  public void process(final StreamsBuilder streamsBuilder) {
    Serde<String> stringSerde = Serdes.String();
    Serde<User> userSerde = UserSerde.UserSerde();
    Serde<Long> longSerde = Serdes.Long();

    KStream<String, User> userStream = streamsBuilder
        .stream("user",
            Consumed.with(stringSerde, userSerde).withOffsetResetPolicy(AutoOffsetReset.EARLIEST));

    userStream.process(() -> new AbstractProcessor<>() {
      @Override
      public void process(String key, User value) {
        log.info("Offset: {} for topic: {} and key: {} at partition: {}", this.context().offset(),
            this.context().topic(), key, this.context().topic());
      }
    });

    KGroupedStream<String, User> user = userStream
        .groupBy((key, value) -> key, Grouped.with(stringSerde, userSerde));

    KTable<String, Long> countKTable = user.count();

    countKTable.toStream().to("user-count", Produced.with(stringSerde, longSerde));

  }

}
