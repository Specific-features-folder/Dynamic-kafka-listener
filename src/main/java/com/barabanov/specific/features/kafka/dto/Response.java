package com.barabanov.specific.features.kafka.dto;

import java.time.OffsetDateTime;

public record Response(String message,
                       OffsetDateTime sendDate) {
}
