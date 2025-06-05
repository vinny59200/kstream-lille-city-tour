package com.vv.kstream.topology;

import com.vv.kstream.model.TimeRange;
import com.vv.kstream.model.VisitStatus;
import com.vv.kstream.serde.VisitStatusSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.LocalTime;
import java.util.List;

import static com.vv.kstream.util.VisitStatusUtils.VALID_TIMETABLES;

public class VisitStatusTopology {

    public Topology build() {
        StreamsBuilder builder = new StreamsBuilder();
        VisitStatusSerde visitStatusSerde = new VisitStatusSerde();

        KStream<String, String> rawVisits = getRawVisitEvents(builder);

        KStream<String, VisitStatus> parsedVisits = parseVisitEvents(rawVisits);

        KStream<String, VisitStatus> validatedVisits = validateVisitStatus(parsedVisits);

        branchAndSend(validatedVisits, visitStatusSerde);

        return builder.build();
    }

    private KStream<String, String> getRawVisitEvents(StreamsBuilder builder) {
        return builder.stream(
                "visit-events",
                Consumed.with(Serdes.String(), Serdes.String()).withName("CSV-data")
                             );
    }

    private KStream<String, VisitStatus> parseVisitEvents(KStream<String, String> visits) {
        return visits.mapValues(value -> {
            String[] parts = value.split(",");
            return new VisitStatus(parts[0], parts[1], parts[2]);
        }, Named.as("VisitStatusBuilder"));
    }

    private KStream<String, VisitStatus> validateVisitStatus(KStream<String, VisitStatus> visits) {
        return visits.mapValues(visit -> {
            boolean isValid = isVisitValid(visit);
            return new VisitStatus(visit.location(), visit.time(), isValid ? "OK" : "NOK");
        }, Named.as("OK-KO-Mapping"));
    }

    private boolean isVisitValid(VisitStatus visit) {
        List<TimeRange> validRanges = VALID_TIMETABLES.getOrDefault(visit.location(), List.of());
        LocalTime visitTime = LocalTime.parse(visit.time());
        return validRanges.stream().anyMatch(range -> range.contains(visitTime));
    }

    private void branchAndSend(KStream<String, VisitStatus> visits, VisitStatusSerde serde) {
        visits.split(Named.as("Split-VisitStatus"))
              .branch(
                      (key, visit) -> "OK".equals(visit.status()),
                      Branched.withConsumer(okStream -> okStream
                              .peek((k, v) -> System.out.println("OK >> " + v), Named.as("printing-OK"))
                              .to("trip-steps", Produced.with(Serdes.String(), serde)))
                     )
              .branch(
                      (key, visit) -> true,
                      Branched.withConsumer(koStream -> koStream
                              .peek((k, v) -> System.out.println("NOK >> " + v), Named.as("printing-KO"))
                              .to("trip-dlq", Produced.with(Serdes.String(), serde)))
                     );
    }
}
