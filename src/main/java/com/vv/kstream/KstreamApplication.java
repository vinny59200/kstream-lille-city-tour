package com.vv.kstream;

import com.vv.kstream.model.TimeRange;
import com.vv.kstream.model.VisitStatus;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.converter.json.GsonBuilderUtils;

import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@SpringBootApplication
public class KstreamApplication {

	public static void main(String[] args) {
		SpringApplication.run(KstreamApplication.class, args);
	}

	@Bean
	public KafkaStreams kafkaStreams() {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "visit-status-app");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

		//		props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
//		props.put( StreamsConfig.producerPrefix( ProducerConfig.COMPRESSION_TYPE_CONFIG ),"gzip" );
//		props.put(StreamsConfig.producerPrefix( ProducerConfig.LINGER_MS_CONFIG ),100);

		StreamsBuilder builder = new StreamsBuilder();

		Serde<VisitStatus> visitStatusSerde =getJsonSerde();

		KStream<String, String> visits = builder.stream( "visit-events",
															  Consumed.with( Serdes.String(), Serdes.String() ).withName( "CSV-data" ) );

		KStream<String, VisitStatus> parsedStream = visits.mapValues(value -> {
			String[] parts = value.split(",");
			return new VisitStatus(parts[0], parts[1], parts[2]);
		}, Named.as( "VisitStatusBuilder" ) );

		parsedStream
				.mapValues(visit -> {
					System.out.println("vv1"+visit.toString());
					System.out.println("vv2>"+visit.location()+"<");
					boolean isValid = VALID_TIMETABLES.getOrDefault(visit.location(), List.of())
													  .stream()
													  .peek(a->System.out.println("vv1"+a.toString()))
													  .anyMatch(range -> range.contains(LocalTime.parse(visit.time())));
					return new VisitStatus(visit.location(), visit.time(), isValid ? "OK" : "NOK");
				}, Named.as("OK-KO-Mapping"))
				.split(Named.as("Split-VisitStatus"))
				.branch(
						(key, visit) -> "OK".equals(visit.status()),
						Branched.withConsumer(okStream -> okStream
								.peek((k, v) -> System.out.println("vv4"+v.toString()), Named.as("printing-OK"))
								.to("trip-steps", Produced.with(Serdes.String(), visitStatusSerde)))
					   )
				.branch(
						(key, visit) -> true,
						Branched.withConsumer(koStream -> koStream
								.peek((k, v) -> System.out.println("vv5"+v.toString()), Named.as("printing-KO"))
								.to("trip-dlq", Produced.with(Serdes.String(), visitStatusSerde)))
					   );


		final Topology topology = builder.build();
		KafkaStreams streams = new KafkaStreams( topology, props);
		System.out.println(topology.describe());
		streams.start();
		return streams;
	}

	private static Serde<VisitStatus> getJsonSerde() {
		Map<String, Object> serdeProps = new HashMap<>();
		serdeProps.put("json.value.type", VisitStatus.class);

		final Serializer<VisitStatus> serializer = new KafkaJsonSerializer<>();
		serializer.configure(serdeProps, false);

		final Deserializer<VisitStatus> deserializer = new KafkaJsonDeserializer<>();
		deserializer.configure(serdeProps, false);

		return Serdes.serdeFrom(serializer, deserializer);
	}

	private static final Map<String, List<TimeRange>> VALID_TIMETABLES = Map.of(
			"Gare Lille Flandres", List.of(new TimeRange("04:35", "00:15")),
			"St Maurice church", List.of(new TimeRange("11:00", "18:00")),
			"Restaurant Les Moules", List.of(new TimeRange("12:00", "15:00"), new TimeRange("19:00", "22:00")),
			"Place Charles de Gaules", List.of(new TimeRange("00:00", "23:59")),
			"Vielle Bourse", List.of(new TimeRange("13:00", "19:00")),
			"Opera", List.of(new TimeRange("01:30", "18:00")),
			"Beffroi", List.of(new TimeRange("10:00", "13:00"), new TimeRange("14:00", "17:30")),
			"Place Louise de Bettignies", List.of(new TimeRange("00:00", "23:59")),
			"Cathedral Notre Dame de la Treille", List.of(new TimeRange("10:30", "18:15"))
																			   );

	private static VisitStatus processVisit( String visit ) {
		try {
			String[] parts = visit.split(",");
			String location = parts[0].trim();
			String time = parts[1].trim();
			LocalTime visitTime = LocalTime.parse( time );

			boolean valid = VALID_TIMETABLES.getOrDefault(location, List.of()).stream()
											.anyMatch(range -> range.contains(visitTime));

			return new VisitStatus(location, time, valid ? "OK" : "KO");

		} catch (Exception e) {
			return new VisitStatus("Invalid", "Invalid", "KO");
		}
	}
}
