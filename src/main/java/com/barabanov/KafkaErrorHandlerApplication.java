package com.barabanov;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;

@SpringBootApplication
public class KafkaErrorHandlerApplication {

	public static void main(String[] args) {
		ConfigurableApplicationContext context = SpringApplication.run(KafkaErrorHandlerApplication.class, args);
		KafkaTemplate<String, Object> kafkaTemplate = context.getBean(KafkaTemplate.class);
		Scanner scanner = new Scanner(System.in);
		ThreadLocalRandom currentRandom = ThreadLocalRandom.current();
		System.out.println(Instant.now().toString());

		if (scanner.nextInt() == 111)
			kafkaTemplate.send("my-topic", new Lada("model-" + currentRandom.nextInt(),
					Instant.now()));
	}

}
