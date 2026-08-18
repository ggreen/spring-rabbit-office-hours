package io.cloudNativeData.spring.rabbit.streams.domain.financial;

import lombok.Builder;

@Builder
public record FixEvent(String id, String event) {
}
