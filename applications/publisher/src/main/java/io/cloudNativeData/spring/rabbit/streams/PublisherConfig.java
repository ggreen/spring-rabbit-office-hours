package io.cloudNativeData.spring.rabbit.streams;

import io.cloudNativeData.spring.rabbit.streams.domain.financial.ActionEvent;
import lombok.extern.slf4j.Slf4j;
import nyla.solutions.core.io.csv.CsvReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

@Configuration
@Slf4j
public class PublisherConfig {

    //@org.springframework.beans.factory.annotation.Value("classpath:csv/additional/events.csv")
    //classpath:csv/financial/events.csv
    //classpath:csv/spring-io-session-events.csv
     @Value("classpath:csv/financial/events.csv")
    private Resource resource;

    @Bean
    Iterator<List<String>> csvLines() throws IOException {
        return new CsvReader(resource.getFile()).stream().iterator();
    }


    @Bean
    Supplier<ActionEvent> eventPublisher(Iterator<List<String>> csvLines) {

        return () -> {
            if(csvLines.hasNext()) {
                var line = csvLines.next();
                log.info("Events {}",line);
                return  ActionEvent.builder()
                        .event(line.getFirst())
                        .account(line.get(1))
                        .time(line.get(2))
                        .id(line.get(3))
                        .build();
            }
            return null;
        };
    }
}
