package io.cloudNativeData.spring.rabbit.streams.domain.financial;

import lombok.Builder;

@Builder
public record ActionEvent(String id, String account, String event, String time) {
}
