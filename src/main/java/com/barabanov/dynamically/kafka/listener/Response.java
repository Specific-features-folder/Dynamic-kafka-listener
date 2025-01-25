package com.barabanov.dynamically.kafka.listener;

import java.io.Serializable;
import java.time.OffsetDateTime;

public record Response(String message,
                       OffsetDateTime sendDate) implements Serializable {
}
