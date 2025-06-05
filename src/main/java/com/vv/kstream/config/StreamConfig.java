package com.vv.kstream.config;

import com.vv.kstream.service.VisitStreamService;
import org.apache.kafka.streams.KafkaStreams;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class StreamConfig {

    @Bean
    public KafkaStreams kafkaStreams(VisitStreamService visitStreamService) {
        return visitStreamService.startVisitStream();
    }
}
