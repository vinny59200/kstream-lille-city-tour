package com.vv.kstream.serde;

import com.vv.kstream.model.VisitStatus;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import org.apache.kafka.common.serialization.Serdes;

import java.util.HashMap;
import java.util.Map;

public class VisitStatusSerde extends Serdes.WrapperSerde<VisitStatus> {

    public VisitStatusSerde() {
        super( new KafkaJsonSerializer<>(), new KafkaJsonDeserializer<>() );

        // Configure the deserializer to use VisitStatus.class
        Map<String, Object> serdeProps = new HashMap<>();
        serdeProps.put("json.value.type", VisitStatus.class);

        this.serializer().configure(serdeProps, false);
        this.deserializer().configure(serdeProps, false);
    }
}