package io.slacklife.serialization;

import io.slacklife.models.User;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

@Slf4j
public class UserSerializer implements Serializer<User> {

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
  public byte[] serialize(String topic, User data) {

    if (null == data) {
      return new byte[0];
    }

    try {
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      ObjectOutputStream outputStream = new ObjectOutputStream(byteArrayOutputStream);
      outputStream.writeObject(data);
      outputStream.flush();
      return byteArrayOutputStream.toByteArray();
    } catch (IOException e) {
      log.error("Unable to serialize object {}", data, e);
      throw new SerializationException(
          "Error when serializing User Data to byte[] due to unsupported encoding " + encoding);
    }

  }
}
