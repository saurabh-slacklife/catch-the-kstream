package io.slacklife.kstreams;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class KStreamsApplication {

	public static void main(String[] args) {
		SpringApplication.run(KStreamsApplication.class, args);
	}

}
