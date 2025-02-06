package com.barabanov.dynamically.kafka.listener;

import com.barabanov.dynamically.kafka.listener.ShardsKafkaConfigs.ShardKafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
import org.springframework.kafka.support.converter.BatchMessagingMessageConverter;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.*;


@Configuration
public class ShardsKafkaConfiguration {

    public static final String REQUEST_TOPIC_PROPERTY = "request-topic";
    public static final String RESPONSE_TOPIC_PROPERTY = "response-topic";


    @Bean
    public Map<String, List<KafkaListenerEndpoint>> shardsKafkaEndpoints(ShardsKafkaConfigs shardsKafkaConfigs,
                                                                         BatchKafkaMessageHandler<Response> resposeBatchKafkaMessageHandler) {

        Map<String, ShardKafkaConfig> shardsKafkaConfigMap = shardsKafkaConfigs.config();
        if (CollectionUtils.isEmpty(shardsKafkaConfigMap))
            return Collections.emptyMap();

        Map<String, List<KafkaListenerEndpoint>> shardsKafkaEndpoints = new HashMap<>();
        for (Map.Entry<String, ShardKafkaConfig> shardKafkaConfig : shardsKafkaConfigMap.entrySet()) {
            String shardName = shardKafkaConfig.getKey();
            List<KafkaListenerEndpoint> endPointsForShard = createKafkaEndPointsForShard(shardName, shardKafkaConfig.getValue(), resposeBatchKafkaMessageHandler);
            shardsKafkaEndpoints.put(shardName, endPointsForShard);
        }

        return shardsKafkaEndpoints;
    }


    private List<KafkaListenerEndpoint> createKafkaEndPointsForShard(String shardName,
                                                                     ShardKafkaConfig shardProperties,
                                                                     BatchKafkaMessageHandler<Response> resposeBatchKafkaMessageHandler) {

        MethodKafkaListenerEndpoint<String, Response> requestKafkaEndPoint = createDefaultMethodKafkaListenerEndpoint(
                shardName,
                shardProperties.topics().get(REQUEST_TOPIC_PROPERTY),
                resposeBatchKafkaMessageHandler,
                "rsp");

        return List.of(requestKafkaEndPoint);
    }


    private <K, V> MethodKafkaListenerEndpoint<K, V> createDefaultMethodKafkaListenerEndpoint(String shardName,
                                                                                              String topic,
                                                                                              BatchKafkaMessageHandler<V> batchKafkaMessageHandler,
                                                                                              String topicAlias) {
        KafkaMessageListenerImpl<K, V> kafkaMessageListener = new KafkaMessageListenerImpl<>(shardName, batchKafkaMessageHandler);

        MethodKafkaListenerEndpoint<K, V> kafkaListenerEndpoint = new MethodKafkaListenerEndpoint<>();
        kafkaListenerEndpoint.setBean(kafkaMessageListener);

        kafkaListenerEndpoint.setId(generateListenerId(shardName, topicAlias));
        kafkaListenerEndpoint.setAutoStartup(true);
        kafkaListenerEndpoint.setTopics(topic);
        kafkaListenerEndpoint.setMessageHandlerMethodFactory(new DefaultMessageHandlerMethodFactory()); //TODO: думаю factory лучше бином

        Method onMessageMethod = ReflectionUtils.findMethod(KafkaMessageListenerImpl.class, "onMessage", List.class);
        kafkaListenerEndpoint.setMethod(Objects.requireNonNull(onMessageMethod));

        return kafkaListenerEndpoint;
    }

    private String generateListenerId(String shardName, String topicAlias) {
        return shardName + "-" + topicAlias;
    }


    @Bean
    public Map<String, ConcurrentKafkaListenerContainerFactory<Object, Object>> shardsKafkaListenerContainerFactories(
            ShardsKafkaConfigs shardsKafkaConfigs) {

        Map<String, ShardKafkaConfig> shardsKafkaConfigMap = shardsKafkaConfigs.config();
        if (CollectionUtils.isEmpty(shardsKafkaConfigMap))
            return Collections.emptyMap();

        Map<String, ConcurrentKafkaListenerContainerFactory<Object, Object>> shardsKafkaListenerContainerFactories = new HashMap<>();
        for (Map.Entry<String, ShardKafkaConfig> shardKafkaConfig : shardsKafkaConfigMap.entrySet()) {
            ShardsKafkaConfigs.ShardKafkaProperties shardKafkaProperties = shardKafkaConfig.getValue().properties();

            DefaultKafkaConsumerFactory<Object, Object> kafkaConsumerFactory = new DefaultKafkaConsumerFactory<>(shardKafkaProperties.buildConsumerProperties());

            ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory = configureListenerContainerFactory(
                    new ConcurrentKafkaListenerContainerFactory<>(),
                    kafkaConsumerFactory,
                    shardKafkaProperties);

            shardsKafkaListenerContainerFactories.put(shardKafkaConfig.getKey(), kafkaListenerContainerFactory);
        }

        return shardsKafkaListenerContainerFactories;
    }

    /**
     * Такая конфигурация не полностью гибкая и часть KafkaProperties не будут работать (та что в блоке listener и не только.
     * Неработающими properties будут properties, задаваемые в ConcurrentKafkaListenerContainerFactoryConfigurer.configure, за исключением concurrency, pollTimeout и type)
     * Для исправления этого необходимо либо вручную конфигурировать ConcurrentKafkaListenerContainerFactory,
     * либо конфигурировать ConcurrentKafkaListenerContainerFactoryConfigurer, как это делается в KafkaAnnotationDrivenConfiguration.
     * Однако все оставшиеся настройки не используются -> не за чем и писать код для использования их в конфигурации.
     *
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

        if (listenerProperties.getType().equals(Type.BATCH)) {
            listenerContainerFactory.setMessageConverter(new BatchMessagingMessageConverter());
            listenerContainerFactory.setBatchListener(true);
        }

        return listenerContainerFactory;
    }


    @Bean
    public Map<String, KafkaTemplate<Object, Object>> shardsKafkaTemplates(ShardsKafkaConfigs shardsKafkaConfigs) {

        Map<String, ShardKafkaConfig> shardsKafkaConfigMap = shardsKafkaConfigs.config();
        if (CollectionUtils.isEmpty(shardsKafkaConfigMap))
            return Collections.emptyMap();

        Map<String, KafkaTemplate<Object, Object>> shardsKafkaTemplates = new HashMap<>();
        for (Map.Entry<String, ShardKafkaConfig> shardKafkaConfig : shardsKafkaConfigMap.entrySet()) {

            DefaultKafkaProducerFactory<Object, Object> kafkaProducerFactory = new DefaultKafkaProducerFactory<>(shardKafkaConfig.getValue().properties().buildProducerProperties());
            KafkaTemplate<Object, Object> kafkaTemplate = new KafkaTemplate<>(kafkaProducerFactory);
            shardsKafkaTemplates.put(shardKafkaConfig.getKey(), kafkaTemplate);
        }

        return shardsKafkaTemplates;
    }


    @Bean
    public Map<String, Map<String, String>> shardsTopicsMap(ShardsKafkaConfigs shardsKafkaConfigs) {

        Map<String, ShardKafkaConfig> shardsKafkaConfigMap = shardsKafkaConfigs.config();
        if (CollectionUtils.isEmpty(shardsKafkaConfigMap))
            return Collections.emptyMap();

        Map<String, Map<String, String>> shardsTopicsMap = new HashMap<>();

        for (Map.Entry<String, ShardKafkaConfig> shardKafkaConfig : shardsKafkaConfigMap.entrySet())
            shardsTopicsMap.put(shardKafkaConfig.getKey(), shardKafkaConfig.getValue().topics());

        return shardsTopicsMap;
    }

}
