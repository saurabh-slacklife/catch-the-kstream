package io.slacklife.serde;

import io.slacklife.models.User;
import io.slacklife.serialization.UserDeserializer;
import io.slacklife.serialization.UserSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class UserSerde extends Serdes {


  static public Serde<User> UserSerde() {
    return Serdes.serdeFrom(new UserSerializer(), new UserDeserializer());
  }

}
