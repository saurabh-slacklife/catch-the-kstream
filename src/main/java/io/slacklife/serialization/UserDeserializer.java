package io.slacklife.serialization;

import io.slacklife.models.User;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

@Slf4j
public class UserDeserializer implements Deserializer<User> {

  private String encoding = "UTF8";

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    String propertyName = isKey ? "key.serializer.encoding" : "value.serializer.encoding";
    Object encodingValue = configs.get(propertyName);
    if (encodingValue == null) {
      encodingValue = configs.get("serializer.encoding");
    }
    if (encodingValue instanceof String) {
      this.encoding = (String) encodingValue;
    }
  }

  @Override
  public User deserialize(String topic, byte[] data) {

    try {
      ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(data));
      User user = (User) objectInputStream.readObject();
      objectInputStream.close();
      return user;
    } catch (IOException | ClassNotFoundException e) {
      log.error("Unable to deserialize byte stream {}", data.toString(), e);
      throw new SerializationException(
          "Error when deserializing byte[] User due to unsupported encoding " + encoding);
    }
  }
}
