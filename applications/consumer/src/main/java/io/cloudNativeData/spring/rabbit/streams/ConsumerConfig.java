package io.cloudNativeData.spring.rabbit.streams;

import com.rabbitmq.client.amqp.Connection;
import com.rabbitmq.client.amqp.Consumer;
import com.rabbitmq.client.amqp.ConsumerBuilder;
import com.rabbitmq.client.amqp.Environment;
import com.rabbitmq.client.amqp.impl.AmqpEnvironmentBuilder;
import io.cloudNativeData.spring.rabbit.streams.domain.financial.ActionEvent;
import io.cloudNativeData.spring.rabbit.streams.domain.financial.AlertList;
import lombok.extern.slf4j.Slf4j;
import nyla.solutions.core.patterns.integration.Subscriber;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.prompt.ChatOptions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.json.JsonMapper;

import java.util.ArrayList;
import java.util.List;

@Configuration
@Slf4j
public class ConsumerConfig {


    //TODO: Add model application properties settings

    private final String prompt = """
                Given the following payment activities, identify potential fraud alerts,
                For each alert response with the "level" with values of (critical, high, medium, low),
                the time and the event which contains why you believe this is an alert
                ONLY RESPONSE the Json Object fields level, time, and event
                ADD details on WHY you identified the alert and the level in the field "event"
            
                [ACTIVITIES]
                ```json
                {listOfActivities}
                ```
                Use Context Below
                CONTEXT:
            
                Activities for the same account from one more than 1 terminal_id within 1 minute time differences is a critical alert.
                Activities for the same account from one more than 1 terminal_id within 5 minute time differences is a high alert.
                Activities with a series of very small transactions (e.g., $0.01 or $1.00) in rapid succession is a high alert
                Activities with merchant_id: MERCH_5555 and amount less than 10 is LOW alert, if amount greater than 100 that create high alert
                Activities for the same account from one more than 1 terminal_id greater than a 1-minute time difference is not an alert
            """;

    private final static String sqlFilter = """
             account = 'john' AND type = 'SALE'
            """;

    @Value("${stream.0.name:events.super.streams.filtering.sql-0}")
    private String streamName0;
    @Value("${stream.1.name:events.super.streams.filtering.sql-1}")
    private String streamName1;


    @Value("${spring.rabbitmq.username}")
    private String username;

    @Value("${spring.rabbitmq.password}")
    private String password;


    @Bean
    Environment amqpEnvironment() {
        return new AmqpEnvironmentBuilder()
                .connectionSettings()
                .username(username)
                .password(password)
                .environmentBuilder()
                .build();
    }

    @Bean
    List<AlertList> events() {
        return new ArrayList<>();
    }

    @Bean
    Connection streamConnection(Environment environment) {
        return environment.connectionBuilder()
                .name("consumer-" + streamName0)
                .build();
    }


    @Bean
    Consumer consumer0(Connection connection,
                       JsonMapper jsonMapper, List<ActionEvent> events, ChatClient chatClient) {
        return constructConsumer(streamName0, connection, jsonMapper, aiEventsConsumer(events, chatClient));

    }

    @Bean
    Consumer consumer1(Connection connection,
                       JsonMapper jsonMapper, List<ActionEvent> events, ChatClient chatClient) {
        return constructConsumer(streamName1, connection, jsonMapper, aiEventsConsumer(events, chatClient));

    }

    @Bean
    ChatClient chatClient(ChatModel chatModel) {

        return ChatClient
                .builder(chatModel)
                .defaultOptions(ChatOptions.builder()
                        .build())
                .build();
    }

    Consumer constructConsumer(String stream,
                               Connection connection,
                               JsonMapper jsonMapper,
                               Subscriber<ActionEvent> subscriber) {

        log.info("input consumed with SQL '{}' from input stream {}", sqlFilter, stream);

        return connection.consumerBuilder()
                .queue(stream)
                .stream()
                .offset(ConsumerBuilder.StreamOffsetSpecification.FIRST)
                .filter()
                .sql(sqlFilter)
                .stream()
                .builder().messageHandler((ctx, inputMessage) -> {
                    try {
                        //Processing input message
                        subscriber.accept(jsonMapper.readValue(inputMessage.body(), ActionEvent.class));

                        //Acknowledge Message acceptance
                        ctx.accept();
                    } catch (Exception e) {
                        log.error("Error:{}", String.valueOf(e));
                        throw e;
                    }
                })
                .build();
    }

    Subscriber<ActionEvent> aiEventsConsumer(List<ActionEvent> events,
                                             ChatClient chatClient) {
        return event -> {
            log.info("Received Event {}", event);
            events.add(event);
            if (events.size() >= 8) {

                log.info("********* Waiting for AI to check for alerts!");

                var alertList = chatClient.prompt()
                        .user(u -> u.text(prompt)
                                .param("listOfActivities", events))
                        .call()
                        .entity(AlertList.class);
                log.info("Model results of alerts: {}", alertList);

                log.info("*********\nalertList: {}\n***************", alertList);
                System.exit(0);
            }
        };
    }
}
