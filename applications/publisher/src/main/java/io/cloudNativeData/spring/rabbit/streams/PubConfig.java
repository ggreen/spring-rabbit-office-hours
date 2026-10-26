package io.cloudNativeData.spring.rabbit.streams;

import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.Message;
import io.cloudNativeData.spring.rabbit.streams.domain.financial.ActionEvent;
import lombok.extern.slf4j.Slf4j;
import nyla.solutions.core.util.Text;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.rabbit.stream.producer.RabbitStreamTemplate;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;

@Configuration
@Slf4j
public class PubConfig {

    /**
     * Loop 17 Million times
     */
    private static final Long loopCount = 17000000L;
    
    @Value("${batchSize:10000}")
    private int batchSize;
    
    @Value("${subBatchSize:5000}")
    private int subBatchSize;
    
    @Value("${perfTestStreamName:perfTest}")
    private String perfTestStreamName;
    
    @Value("${eventMessage:id:01|payment:30.00|type:AMT}")
    private String eventMessage;
    
    
    @Bean
    RabbitStreamTemplate template(Environment environment) {
    
        //Create Stream
        environment.streamCreator().stream(perfTestStreamName).create();
    
        var template = new RabbitStreamTemplate(environment, perfTestStreamName);
        template.setProducerCustomizer((stream, builder) -> {
            //Local batch size before sending
            builder.batchSize(batchSize);
            //Number of messages to send per batch
            builder.subEntrySize(subBatchSize);
        });
    
        return template;
    }
    
    
    @Bean
    Message msg(RabbitStreamTemplate template, JsonMapper mapper) {
        var event = ActionEvent.builder().event(eventMessage).build();
        var msgPayload = mapper.writeValueAsString(event);
        log.info("Sending event: {}", msgPayload);
    
        return template.messageBuilder().addData(msgPayload.getBytes(StandardCharsets.UTF_8))
                .build();
    }
    
    @Bean
    ApplicationRunner applicationRunner(RabbitStreamTemplate template, Message streamMsg) {
        return args -> {
    
            var start = System.currentTimeMillis();
    
            for (int i = 0; i < loopCount; i++) {
    
                template.send(streamMsg);
            }
    
            var end = System.currentTimeMillis();
            var thruPut = (loopCount / (end - start)) * 1000;
            
            log.info("{} per second", Text.format().formatNumber(thruPut));
        };
    }
}
