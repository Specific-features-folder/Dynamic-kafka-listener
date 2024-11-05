package com.barabanov;

import java.time.Instant;

public record Lada(
        String model,
        Instant createDate) {
}
