package com.vv.kstream.service;

import com.vv.kstream.topology.VisitStatusTopology;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.*;

class VisitStreamServiceTest {

    VisitStreamService service;

    @BeforeEach
    void setup() {
        service = new VisitStreamService();
    }

    @Test
    void shouldStartKafkaStreams() {
        Topology mockTopology = mock(Topology.class);

        try (MockedConstruction<KafkaStreams> mocked = mockConstruction(KafkaStreams.class,
                                                                        (mock, context) -> {
                                                                            doNothing().when(mock).start();
                                                                        })) {

            // Mock VisitStatusTopology directly since it is instantiated in the service
            VisitStatusTopology mockedTopology = mock(VisitStatusTopology.class);
            when(mockedTopology.build()).thenReturn(mockTopology);

            KafkaStreams result = service.startVisitStream();

            assertNotNull(result);
            KafkaStreams constructedKafkaStreams = mocked.constructed().get(0);
            assertSame(constructedKafkaStreams, result);
            verify(constructedKafkaStreams).start();
        }
    }
}
