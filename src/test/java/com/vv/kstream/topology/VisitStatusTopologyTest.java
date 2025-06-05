package com.vv.kstream.topology;

import com.vv.kstream.model.VisitStatus;
import com.vv.kstream.serde.VisitStatusSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.*;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class VisitStatusTopologyTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, VisitStatus> okOutputTopic;
    private TestOutputTopic<String, VisitStatus> koOutputTopic;
    private final String inputTopicName = "visit-events";
    private final String okTopicName = "trip-steps";
    private final String koTopicName = "trip-dlq";

    private final Serde<String> stringSerde = Serdes.String();
    private final Serde<VisitStatus> visitStatusSerde = new VisitStatusSerde();

    @BeforeEach
    void setup() {
        VisitStatusTopology topology = new VisitStatusTopology();
        Topology kafkaTopology = topology.build();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-visit-status-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        testDriver = new TopologyTestDriver(kafkaTopology, props);

        inputTopic = testDriver.createInputTopic(inputTopicName, stringSerde.serializer(), stringSerde.serializer());
        okOutputTopic = testDriver.createOutputTopic(okTopicName, stringSerde.deserializer(), visitStatusSerde.deserializer());
        koOutputTopic = testDriver.createOutputTopic(koTopicName, stringSerde.deserializer(), visitStatusSerde.deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testValidVisitGoesToOkTopic() {
        // Given a valid visit event within timetable range (e.g. Place Louise de Bettignies is always valid)
        String input = "Place Louise de Bettignies,12:00,OK";

        // When sending input record
        inputTopic.pipeInput(null, input);

        // Then output in OK topic with status "OK"
        assertFalse(okOutputTopic.isEmpty());
        VisitStatus visitStatus = okOutputTopic.readValue();
        assertEquals("Place Louise de Bettignies", visitStatus.location());
        assertEquals("12:00", visitStatus.time());
        assertEquals("OK", visitStatus.status());

        // NOK topic should be empty
        assertTrue(koOutputTopic.isEmpty());
    }

    @Test
    void testInvalidVisitGoesToKoTopic() {
        // Given an invalid visit event (e.g. Beffroi at 13:30 is outside its valid ranges)
        String input = "Beffroi,13:30,NOK";

        // When sending input record
        inputTopic.pipeInput(null, input);

        // Then output in NOK topic with status "NOK"
        assertFalse(koOutputTopic.isEmpty());
        VisitStatus visitStatus = koOutputTopic.readValue();
        assertEquals("Beffroi", visitStatus.location());
        assertEquals("13:30", visitStatus.time());
        assertEquals("NOK", visitStatus.status());

        // OK topic should be empty
        assertTrue(okOutputTopic.isEmpty());
    }

    @Test
    void testStatusIsRecomputedByTopology() {
        // Even if input status is incorrect, topology recomputes it based on time ranges
        String input = "Beffroi,12:00,NOK"; // 12:00 is valid for Beffroi (10:00-13:00), so status should be OK

        inputTopic.pipeInput(null, input);

        // Should be routed to OK topic with status corrected to "OK"
        assertFalse(okOutputTopic.isEmpty());
        VisitStatus visitStatus = okOutputTopic.readValue();
        assertEquals("Beffroi", visitStatus.location());
        assertEquals("12:00", visitStatus.time());
        assertEquals("OK", visitStatus.status());

        assertTrue(koOutputTopic.isEmpty());
    }
}
