package io.cloudNativeData.spring.rabbit.streams;

import jakarta.annotation.Nullable;
import org.jspecify.annotations.NonNull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.MessageBuilder;
import tools.jackson.databind.json.JsonMapper;

@Configuration
public class SerializationConfig {

//    private final static String ClassTypeHeaderName = "serializationClassType";


    @Bean
    MessageConverter messageConverter(JsonMapper jsonMapper) {
        return new MessageConverter() {
            @Override
            public @Nullable Object fromMessage(@NonNull Message<?> message, @NonNull Class<?> targetClass) {

                var payload = message.getPayload();
                if (payload instanceof byte[] bytes) {
                    return jsonMapper.readValue(bytes, targetClass);
                }
                return payload;
            }

            @Override
            public @NonNull Message<?> toMessage(@NonNull Object payload, @NonNull MessageHeaders headers) {

//                if (payload instanceof byte[] bytes) {
//
//                    String classTypeHeaderText = String.valueOf(headers.get(ClassTypeHeaderName));
//                    var classType = Class.forName(classTypeHeaderText);
//
//                    payload = jsonMapper.readValue(bytes,classType);
//                }


                var builder = MessageBuilder.withPayload(payload);

                builder.setHeader(MessageHeaders.CONTENT_TYPE,
                        MediaType.APPLICATION_JSON.toString()).build();

//                builder.setHeader(ClassTypeHeaderName,payload.getClass().getName());

                return builder.build();
            }
        };
    }
}
