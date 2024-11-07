package com.barabanov;

import com.sun.istack.NotNull;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.BackOffExecution;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;


@Slf4j
@RequiredArgsConstructor
public class MyFailedRecordTracker implements RecoveryStrategy {

    private final ThreadLocal<Map<TopicPartition, FailedRecord>> failures = new ThreadLocal<>(); // intentionally not static

    private final ConsumerAwareRecordRecoverer recoverer;

    private final boolean noRetries;

    private final BackOff backOff;

    private final BackOffHandler backOffHandler;


    MyFailedRecordTracker(BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer, BackOff backOff) {

        this(recoverer, backOff, null);
    }


    MyFailedRecordTracker(BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer,
                          @NotNull BackOff backOff,
                          BackOffHandler backOffHandler) {

        Assert.notNull(backOff, "'backOff' cannot be null");
        if (recoverer == null) {
            this.recoverer = (rec, consumer, thr) -> {
                Map<TopicPartition, FailedRecord> map = this.failures.get();
                FailedRecord failedRecord = null;
                if (map != null) {
                    failedRecord = map.get(new TopicPartition(rec.topic(), rec.partition()));
                }
                log.error("Exception: " + thr
                        + "\nBackoff "
                        + (failedRecord == null
                        ? "none"
                        : failedRecord.getBackOffExecution())
                        + " exhausted for " + KafkaUtils.format(rec));
            };
        } else {
            if (recoverer instanceof ConsumerAwareRecordRecoverer) {
                this.recoverer = (ConsumerAwareRecordRecoverer) recoverer;
            } else {
                this.recoverer = (rec, consumer, ex) -> recoverer.accept(rec, ex);
            }
        }
        this.noRetries = backOff.start().nextBackOff() == BackOffExecution.STOP;
        this.backOff = backOff;

        this.backOffHandler = backOffHandler == null ? new DefaultBackOffHandler() : backOffHandler;
    }


    @Override
    public boolean recovered(ConsumerRecord<?, ?> record, Exception exception,
                             @Nullable MessageListenerContainer container,
                             @Nullable Consumer<?, ?> consumer) {

        if (this.noRetries) {
            attemptRecovery(record, exception, null, consumer);
            return true;
        }
        Map<TopicPartition, FailedRecord> map = this.failures.get();
        if (map == null) {
            this.failures.set(new HashMap<>());
            map = this.failures.get();
        }
        TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
        FailedRecord failedRecord = getFailedRecordInstance(record, exception, map, topicPartition);
        long nextBackOff = failedRecord.getBackOffExecution().nextBackOff();
        if (nextBackOff != BackOffExecution.STOP) {
            this.backOffHandler.onNextBackOff(container, exception, nextBackOff);
            return false;
        } else {
            attemptRecovery(record, exception, topicPartition, consumer);
            map.remove(topicPartition);
            if (map.isEmpty()) {
                this.failures.remove();
            }
            return true;
        }
    }


    private FailedRecord getFailedRecordInstance(ConsumerRecord<?, ?> record, Exception exception,
                                                 Map<TopicPartition, FailedRecord> map, TopicPartition topicPartition) {

        Exception realException = exception;
        if (realException instanceof ListenerExecutionFailedException
                && realException.getCause() instanceof Exception) {

            realException = (Exception) realException.getCause();
        }
        FailedRecord failedRecord = map.get(topicPartition);
        if (failedRecord == null || failedRecord.getOffset() != record.offset()
                || (!realException.getClass().isInstance(failedRecord.getLastException()))) {

            failedRecord = new FailedRecord(record.offset(), this.backOff.start());
            map.put(topicPartition, failedRecord);
        } else {
            failedRecord.getDeliveryAttempts().incrementAndGet();
        }
        failedRecord.setLastException(realException);
        return failedRecord;
    }


    private void attemptRecovery(ConsumerRecord<?, ?> record,
                                 Exception exception,
                                 TopicPartition tp,
                                 Consumer<?, ?> consumer) {

        try {
            this.recoverer.accept(record, consumer, exception);
        } catch (RuntimeException e) {
            if (tp != null) {
                this.failures.get().remove(tp);
            }
            throw e;
        }
    }


    @Getter
    @RequiredArgsConstructor
    private static final class FailedRecord {

        private final long offset;

        private final BackOffExecution backOffExecution;

        private final AtomicInteger deliveryAttempts = new AtomicInteger(1);

        @Setter
        private Exception lastException;
    }

}