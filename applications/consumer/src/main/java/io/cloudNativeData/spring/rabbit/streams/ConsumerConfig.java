package io.cloudNativeData.spring.rabbit.streams;

import com.rabbitmq.stream.OffsetSpecification;
import io.cloudNativeData.spring.rabbit.streams.domain.financial.ActionEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.rabbit.stream.listener.StreamListenerContainer;

import java.util.function.Consumer;

@Configuration
@Slf4j
public class ConsumerConfig {

    @Bean
    ListenerContainerCustomizer<MessageListenerContainer> customizer() {
        return (msgListenerContainer, dest, group) -> {
            if (msgListenerContainer instanceof StreamListenerContainer streamContainer) {
                streamContainer.setConsumerCustomizer((name, builder) -> {
                    builder.subscriptionListener(
                            subscriptionContext -> subscriptionContext
                                    .offsetSpecification(OffsetSpecification.first()));

                });
            }
        };
    }

    @Bean
    Consumer<ActionEvent> logConsumer() {
        return event -> {
            log.info("Received: {}", event);
        };
    }
}
