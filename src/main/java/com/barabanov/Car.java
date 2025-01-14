package com.barabanov;

import java.time.Instant;

public record Car(
        String model,
        Instant createDate) {
}
