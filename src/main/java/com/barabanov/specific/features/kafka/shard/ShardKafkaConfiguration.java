package com.barabanov.specific.features.kafka.shard;

import com.barabanov.specific.features.kafka.dto.Request;
import com.barabanov.specific.features.kafka.shard.ShardsConfig.ShardConfig;
import com.barabanov.specific.features.kafka.shard.ShardsConfig.ShardKafkaConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties.Listener.Type;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.*;

/**
 * При использовании Map из этой конфигурации лучше указывать @Qualifier т.к., например, такую Map<String, KafkaTemplate<Object, Object>>
 * spring может создать и из KafkaTemplate от авто конфигурации и без ошибок внедрить
 * <p>
 * В некоторых местах в этой конфигурации вместо создания бинов и их переиспользования создаются новые объекты для каждой кафки.
 * Выбран такой подход из-за отсутствия уверенности в том, что один и тот же объект способен корректно работать с несколькими кафками одновременно.
 */
@Slf4j
@Configuration
public class ShardKafkaConfiguration {

    private final List<ShardConfig> shardConfigsList;
    private final KafkaProperties shardKafkaProperties;

    public ShardKafkaConfiguration(ShardsConfig shardsConfig) {
        this.shardConfigsList = shardsConfig.configs();
        this.shardKafkaProperties = shardsConfig.kafkaProperties();
    }


    @Bean
    public Map<String, List<KafkaListenerEndpoint>> shardsKafkaEndpoints(BatchShardKafkaMsgHandler<Request> shardRequestKafkaMsgHandler) {

        Map<String, List<KafkaListenerEndpoint>> shardsKafkaEndpoints = new HashMap<>();
        for (ShardConfig shardConfig : shardConfigsList) {
            String shardName = shardConfig.shardName();
            List<KafkaListenerEndpoint> endPointsForShard = createKafkaEndPointsForShard(
                    shardName,
                    shardConfig.kafkaConfig(),
                    shardRequestKafkaMsgHandler);
            shardsKafkaEndpoints.put(shardName, endPointsForShard);
        }

        return shardsKafkaEndpoints;
    }


    /**
     * Создаёт всех kafka endpoint'ы для конкретной шарды (создаёт кафка слушателей и связывает их с обработчиками).
     * Соответственно, если нужно слушать не один топик, а n, то нужно в этом методе сделать n вызовов создания kafka endpoint батчевого или нет.
     * Если нужен не батчевый слушатель, то тогда нужно в shardsKafkaEndpoints принимать ShardKafkaMsgHandler вместо батчевого или в дополнение к нему и связывать в этом методе слушателя и топик
     */
    private List<KafkaListenerEndpoint> createKafkaEndPointsForShard(String shardName,
                                                                     ShardKafkaConfig shardKafkaConfig,
                                                                     BatchShardKafkaMsgHandler<Request> shardRequestKafkaMsgHandler) {

        log.info("Создаётся batch kafka endpoint для шарды: {} для топика {}", shardName, shardKafkaConfig.requestTopicName());
        MethodKafkaListenerEndpoint<String, Request> requestKafkaEndPoint = createBatchMethodKafkaListenerEndpoint(
                shardName,
                shardKafkaConfig.requestTopicName(),
                shardRequestKafkaMsgHandler,
                Request.class,
                "rq");

        return List.of(requestKafkaEndPoint);
    }


    /**
     * Задаются конкретные значения, аналогичные параметрам из @KafkaListener.
     * Для каждого kafka listener можно задать default type, concurrency, topics, batch и т.д.
     */
    private <K, V> MethodKafkaListenerEndpoint<K, V> createBatchMethodKafkaListenerEndpoint(String shardName,
                                                                                            String topic,
                                                                                            BatchShardKafkaMsgHandler<V> batchShardKafkaMsgHandler,
                                                                                            Class<V> defaultMsgClass,
                                                                                            String topicAlias) {

        MethodKafkaListenerEndpoint<K, V> kafkaListenerEndpoint = new MethodKafkaListenerEndpoint<>();
        kafkaListenerEndpoint.setId(generateListenerId(shardName, topicAlias));
        kafkaListenerEndpoint.setAutoStartup(true);
        kafkaListenerEndpoint.setTopics(topic);
        kafkaListenerEndpoint.setBatchListener(true);
        kafkaListenerEndpoint.setMessageHandlerMethodFactory(new DefaultMessageHandlerMethodFactory());

        BatchKafkaMsgListenerImpl<K, V> kafkaMessageListener = new BatchKafkaMsgListenerImpl<>(shardName, batchShardKafkaMsgHandler);
        kafkaListenerEndpoint.setBean(kafkaMessageListener);
        Method onMessageMethod = ReflectionUtils.findMethod(BatchKafkaMsgListenerImpl.class, "onMessage", List.class);
        kafkaListenerEndpoint.setMethod(Objects.requireNonNull(onMessageMethod));

        Properties endPoindProperties = new Properties();
        endPoindProperties.put(JsonDeserializer.VALUE_DEFAULT_TYPE, defaultMsgClass.getName());
        kafkaListenerEndpoint.setConsumerProperties(endPoindProperties);
        // Если не будет хватать общей настройки kafka.listener.concurrency,
        // то можно ещё тут concurrency задавать для каждого конкретного end pont, если это нужно (kafkaListenerEndpoint.setConcurrency()).

        return kafkaListenerEndpoint;
    }


    private <K, V> MethodKafkaListenerEndpoint<K, V> createMethodKafkaListenerEndpoint(String shardName,
                                                                                       String topic,
                                                                                       ShardKafkaMsgHandler<V> shardKafkaMsgHandler,
                                                                                       Class<V> defaultMsgClass,
                                                                                       String topicAlias) {

        MethodKafkaListenerEndpoint<K, V> kafkaListenerEndpoint = new MethodKafkaListenerEndpoint<>();
        kafkaListenerEndpoint.setId(generateListenerId(shardName, topicAlias));
        kafkaListenerEndpoint.setAutoStartup(true);
        kafkaListenerEndpoint.setTopics(topic);
        kafkaListenerEndpoint.setBatchListener(false);
        kafkaListenerEndpoint.setMessageHandlerMethodFactory(new DefaultMessageHandlerMethodFactory());

        KafkaMsgListenerImpl<K, V> kafkaMessageListener = new KafkaMsgListenerImpl<>(shardName, shardKafkaMsgHandler);
        kafkaListenerEndpoint.setBean(kafkaMessageListener);
        Method onMessageMethod = ReflectionUtils.findMethod(BatchKafkaMsgListenerImpl.class, "onMessage", List.class);
        kafkaListenerEndpoint.setMethod(Objects.requireNonNull(onMessageMethod));

        Properties endPoindProperties = new Properties();
        endPoindProperties.put(JsonDeserializer.VALUE_DEFAULT_TYPE, defaultMsgClass.getName());
        kafkaListenerEndpoint.setConsumerProperties(endPoindProperties);
        // Если не будет хватать общей настройки kafka.listener.concurrency,
        // то можно ещё тут concurrency задавать для каждого конкретного end pont, если это нужно (kafkaListenerEndpoint.setConcurrency()).

        return kafkaListenerEndpoint;
    }

    private String generateListenerId(String shardName, String topicAlias) {
        return shardName + "-" + topicAlias;
    }


    @Bean
    public Map<String, ConcurrentKafkaListenerContainerFactory<Object, Object>> shardsKafkaListenerContainerFactories() {

        Map<String, ConcurrentKafkaListenerContainerFactory<Object, Object>> shardsKafkaListenerContainerFactories = new HashMap<>();
        for (ShardConfig shardConfig : this.shardConfigsList) {
            // Это не очень хорошо, но у KafkaProperties нет копирующего конструктора, а при таком использовании вроде бы не возникает проблем
            this.shardKafkaProperties.setBootstrapServers(shardConfig.kafkaConfig().bootstrapServers());

            DefaultKafkaConsumerFactory<Object, Object> kafkaConsumerFactory = new DefaultKafkaConsumerFactory<>(this.shardKafkaProperties.buildConsumerProperties());
            ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory = configureListenerContainerFactory(
                    new ConcurrentKafkaListenerContainerFactory<>(),
                    kafkaConsumerFactory,
                    this.shardKafkaProperties);

            shardsKafkaListenerContainerFactories.put(shardConfig.shardName(), kafkaListenerContainerFactory);
            this.shardKafkaProperties.setBootstrapServers(null);
        }

        return shardsKafkaListenerContainerFactories;
    }

    /**
     * Такая конфигурация не полностью гибкая и часть KafkaProperties не будут работать (та что в блоке listener и не только.
     * Неработающими properties будут properties, задаваемые в ConcurrentKafkaListenerContainerFactoryConfigurer.configure, за исключением concurrency, pollTimeout и type)
     * Для исправления этого необходимо либо вручную конфигурировать ConcurrentKafkaListenerContainerFactory,
     * либо конфигурировать ConcurrentKafkaListenerContainerFactoryConfigurer, как это делается в KafkaAnnotationDrivenConfiguration.
     * Однако все оставшиеся настройки не используются -> не за чем и писать код для использования их в конфигурации.
     * <p>
     * Внимание! Возможно, что при добавлении различных обработчиков ошибок стоит учитывать батчевый ли будет контейнер или нет, как это делается в
     * ConcurrentKafkaListenerContainerFactoryConfigurer.configure. (хотя скорее всего это ни на что не влияет и просто не будет использовать обработчик, если его лишним задать)
     */
    private ConcurrentKafkaListenerContainerFactory<Object, Object> configureListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactory<Object, Object> listenerContainerFactory,
            DefaultKafkaConsumerFactory<Object, Object> consumerFactory,
            KafkaProperties kafkaProperties) {
        listenerContainerFactory.setConsumerFactory(consumerFactory);

        KafkaProperties.Listener listenerProperties = kafkaProperties.getListener();
        Optional.ofNullable(listenerProperties.getConcurrency()).ifPresent(listenerContainerFactory::setConcurrency);
        Optional.ofNullable(listenerProperties.getPollTimeout()).ifPresent(pollTimeoutDuration ->
                listenerContainerFactory.getContainerProperties().setPollTimeout(pollTimeoutDuration.toMillis()));

        if (listenerProperties.getType().equals(Type.BATCH))
            listenerContainerFactory.setBatchListener(true);

        return listenerContainerFactory;
    }


    @Bean
    public Map<String, KafkaTemplate<String, Object>> shardsKafkaTemplates() {

        Map<String, KafkaTemplate<String, Object>> shardsKafkaTemplates = new HashMap<>();
        for (ShardConfig shardConfig : this.shardConfigsList) {
            String shardName = shardConfig.shardName();
            log.info("Создаётся KafkaTemplate для шарды {}", shardName);

            this.shardKafkaProperties.setBootstrapServers(shardConfig.kafkaConfig().bootstrapServers());
            DefaultKafkaProducerFactory<String, Object> kafkaProducerFactory = new DefaultKafkaProducerFactory<>(this.shardKafkaProperties.buildProducerProperties());
            KafkaTemplate<String, Object> kafkaTemplate = new KafkaTemplate<>(kafkaProducerFactory);
            shardsKafkaTemplates.put(shardName, kafkaTemplate);

            this.shardKafkaProperties.setBootstrapServers(null);
        }

        return shardsKafkaTemplates;
    }


    @Bean
    public Map<String, Map<ShardWritableTopic, String>> shardsWritableTopicsMap() {

        Map<String, Map<ShardWritableTopic, String>> shardsTopicsMap = new HashMap<>();

        for (ShardConfig shardConfig : this.shardConfigsList) {
            Map<ShardWritableTopic, String> shardWritableTopicMap = new HashMap<>();
            shardWritableTopicMap.put(ShardWritableTopic.RESPONSE_TOPIC, shardConfig.kafkaConfig().responseTopicName());

            shardsTopicsMap.put(shardConfig.shardName(), shardWritableTopicMap);
        }

        return shardsTopicsMap;
    }

}
