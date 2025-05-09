package com.barabanov.specific.features;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@ConfigurationPropertiesScan
@SpringBootApplication
public class DynamicKafkaListenerApplication {

    public static void main(String[] args) {
        SpringApplication.run(DynamicKafkaListenerApplication.class, args);
    }

}
