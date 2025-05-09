package com.barabanov.specific.features.kafka.dto;

import java.time.OffsetDateTime;

public record Request(String message,
                      OffsetDateTime sendDate) {
}
