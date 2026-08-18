package io.cloudNativeData.spring.rabbit.streams;

import com.rabbitmq.stream.Environment;
import io.cloudNativeData.spring.rabbit.streams.domain.financial.FixEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.rabbit.stream.producer.RabbitStreamTemplate;
import tools.jackson.databind.json.JsonMapper;

@Configuration
@Slf4j
public class PublisherConfig {

    @Bean
    RabbitStreamTemplate rabbitStreamTemplate(Environment environment,
                                              JsonMapper jsonMapper)
    {
        var template = new RabbitStreamTemplate(environment,"events.spring.io");
        template.setMessageConverter(new JacksonJsonMessageConverter(jsonMapper));

        return template;
    }

    @Bean
    ApplicationRunner applicationRunner(RabbitStreamTemplate rabbitStreamTemplate) {
        return args -> {
            log.info("Publishing Spring IO events");
            rabbitStreamTemplate.convertAndSend(FixEvent.builder().event("Welcome to RabbitMQ session")
                    .build());
        };
    }
}
