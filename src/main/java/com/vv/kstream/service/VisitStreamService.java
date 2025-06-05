package com.vv.kstream.service;

import com.vv.kstream.topology.VisitStatusTopology;
import com.vv.kstream.util.VisitEventProducerUtil;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Properties;

@Service
public class VisitStreamService {

    public KafkaStreams startVisitStream() {
        List<String> events = VisitEventProducerUtil.readEventsFromResource( "/visit-events.csv" );
        VisitEventProducerUtil.produceEvents( "localhost:9092", "visit-events", events );


        Properties props = new Properties();
        props.put( StreamsConfig.APPLICATION_ID_CONFIG, "visit-status-app" );
        props.put( StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092" );
        props.put( StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String()
                                                                       .getClass() );
        props.put( StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100 );
        props.put( StreamsConfig.producerPrefix( ProducerConfig.COMPRESSION_TYPE_CONFIG ), "gzip" );

        // Optional advanced tuning
        // props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        // props.put(StreamsConfig.producerPrefix(ProducerConfig.LINGER_MS_CONFIG), 100);

        Topology topology = new VisitStatusTopology().build();
        KafkaStreams streams = new KafkaStreams( topology, props );

        System.out.println( topology.describe() );

        streams.start();
        return streams;
    }
}
