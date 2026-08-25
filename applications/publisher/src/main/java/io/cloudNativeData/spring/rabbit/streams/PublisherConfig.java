package io.cloudNativeData.spring.rabbit.streams;

import com.rabbitmq.stream.Environment;
import io.cloudNativeData.spring.rabbit.streams.domain.financial.ActionEvent;
import lombok.extern.slf4j.Slf4j;
import nyla.solutions.core.io.csv.CsvReader;
import nyla.solutions.core.util.Text;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.rabbit.stream.producer.RabbitStreamTemplate;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

@Configuration
@Slf4j
public class PublisherConfig {





    //@org.springframework.beans.factory.annotation.Value("classpath:csv/additional/events.csv")
    @Value("classpath:csv/financial/events.csv")
    //@org.springframework.beans.factory.annotation.Value("classpath:csv/spring-io-session-events.csv")
    private Resource resource;

    @Bean
    Iterator<List<String>> csvLines() throws IOException {
        return new CsvReader(resource.getFile()).stream().iterator();
    }

    //TODO: Publisher application.properties MUST match the super stream name
    @Bean
    Supplier<Message<ActionEvent>> eventPublisher(Iterator<List<String>> csvLines) {

        return () -> {
            if (csvLines.hasNext()) {
                var line = csvLines.next();
                var event = ActionEvent.builder()
                        .event(line.get(0))
                        .account(line.get(1))
                        .type(line.get(2))
                        .time(line.get(3))
                        .id(line.get(4))
                        .build();

                log.info("sending: {}",event);

                return MessageBuilder.withPayload(event)
                        .setHeader("account", event.account())
                        .setHeader("type", event.type())
                        .build();
            }
            return null;
        };
    }
    
}
