package io.slacklife.kstreams.components;

import io.slacklife.models.User;
import io.slacklife.serde.UserSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class StreamReadProcessor {

  @Autowired
  public void process(final StreamsBuilder streamsBuilder) {
    Serde<String> stringSerde = Serdes.String();
    Serde<User> userSerde = UserSerde.UserSerde();
    Serde<Long> longSerde = Serdes.Long();

    KGroupedStream<String, User> user = streamsBuilder
        .stream("user", Consumed.with(stringSerde, userSerde))
        .groupBy((key, value) -> key, Grouped.with(stringSerde, userSerde));

    KTable<String, Long> countKTable = user.count();

    countKTable.toStream().to("user-count", Produced.with(stringSerde, longSerde));
  }

}
