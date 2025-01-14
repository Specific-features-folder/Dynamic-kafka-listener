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

//		List<String> modelNames = List.of("Vaz", "Lada", "Gaz", "Yaz", "Belaz", "Kamaz");
//		modelNames.forEach(name -> {
//			kafkaTemplate.send("car-topic", new Car(name,
//					Instant.now()));
//		});
//		for (int i = 0; i < 4_000_000; i++) {
//			kafkaTemplate.send("car-topic", new Car("model-" + i,
//					Instant.now()));
//		}
//		while (scanner.nextLine().equals("send"))
//			kafkaTemplate.send("car-topic", new Car("model-" + currentRandom.nextInt(),
//					Instant.now()));
	}

}
