import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Serialized;

import java.util.Arrays;
import java.util.Properties;

public class A4Application {

    public static void main(String[] args) throws Exception {
		// do not modify the structure of the command line
		String bootstrapServers = args[0];
		String appName = args[1];
		String studentTopic = args[2];
		String classroomTopic = args[3];
		String outputTopic = args[4];
		String stateStoreDir = args[5];

		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.STATE_DIR_CONFIG, stateStoreDir);

		// add code here if you need any additional configuration options

		final Serde<String> stringSerde = Serdes.String();
		final Serde<Long> longSerde = Serdes.Long();

		StreamsBuilder builder = new StreamsBuilder();

		// classrooms input stream
		KTable<String, String> classroomInputStream = builder.table(classroomTopic);
		KTable<String, Long> classCapacity = classroomInputStream
			.mapValues((v) -> Long.parseLong(String.valueOf(v)));

		// students input stream
		KStream<String, String> studentInputStream = builder.stream(studentTopic);
		KTable<String, Long> studentOccupancy = studentInputStream
			.groupByKey()
			.reduce((k, v) -> v)
			.groupBy((studentID, roomID) -> KeyValue.pair(String.valueOf(roomID), studentID))
			.count();		
		
		// combine occupancy/capacity streams
		KStream<String, Room> roomOccupancyStream = studentOccupancy
			.toStream()
			.leftJoin(classCapacity, (occupants, capacity) -> new Room(capacity, occupants));
		KStream<String, Room> roomCapacityStream = classCapacity
			.toStream()
			.leftJoin(studentOccupancy, (occupants, capacity) -> new Room(capacity, occupants));
		KStream<String, Room> updateStream = roomOccupancyStream.merge(roomCapacityStream);

		// find overflow rooms
		KTable<String, Long> overflowStream = updateStream
			.mapValues((room) -> room.occupancy - room.capacity)
			.groupByKey(Serialized.with(stringSerde, longSerde))
			.reduce((k, v) -> v);

		// calculate output
		KStream<String, String> output = updateStream
			.join(overflowStream, (room, diff) -> {
				System.out.println(diff);
				System.out.println(room.occupancy - room.capacity);
				room.setPrevDiff(diff);
				return room;
				})
			.filter((roomID, room) -> {
				
				
				return room.occupancy > room.capacity || room.prevDiff > 0;
				})
			.map((roomID, room) -> {
				Long diff = room.occupancy - room.capacity;
				String val = diff == 0 ? "OK" : String.valueOf(room.occupancy);
				return KeyValue.pair(roomID, val);
			});

		output.to(outputTopic, Produced.with(stringSerde, stringSerde));

		KafkaStreams streams = new KafkaStreams(builder.build(), props);

		// this line initiates processing
		streams.start();

		// shutdown hook for Ctrl+C
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
	}
	
	static class Room {
		public Long capacity;
		public Long occupancy;
		public Long prevDiff;

		public Room(Long capacity, Long occupancy){
			this.capacity = capacity != null ? capacity : Long.MAX_VALUE;
			this.occupancy = occupancy != null ? occupancy : 0L;
			this.prevDiff = 0L;
		}

		public void setPrevDiff(Long diff){
			this.prevDiff = diff;
		}
	}

	static class NextRoom {
		public Room room;
		public Long overflow;

		public NextRoom(Room room, Long overflow){
			this.room = room;
			this.overflow = overflow;
		}
	}
}
