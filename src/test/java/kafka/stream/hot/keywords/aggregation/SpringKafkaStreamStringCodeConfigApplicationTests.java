package kafka.stream.hot.keywords.aggregation;

import kafka.stream.hot.keywords.aggregation.config.Constants;
import kafka.stream.hot.keywords.aggregation.processor.KafkaProcessor;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Properties;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;


@SpringBootTest
class SpringKafkaStreamStringCodeConfigApplicationTests {

	@Test
	void givenInputMessages_whenProcessed_thenWordCountIsProduced() {
		KafkaProcessor wordCountProcessor = new KafkaProcessor();

		StreamsBuilder streamsBuilder = new StreamsBuilder();
		wordCountProcessor.kStream(streamsBuilder);
		Topology topology = streamsBuilder.build();

		try (TopologyTestDriver topologyTestDriver = new TopologyTestDriver(topology, new Properties())) {
			TestInputTopic<String, String> inputTopic = topologyTestDriver
					.createInputTopic(Constants.INPUT_TOPIC, new StringSerializer(), new StringSerializer());

			TestOutputTopic<String, Long> outputTopic = topologyTestDriver
					.createOutputTopic(Constants.OUTPUT_TOPIC, new StringDeserializer(), new LongDeserializer());

			inputTopic.pipeInput("key", "hello world");
			inputTopic.pipeInput("key2", "hello");
			inputTopic.pipeInput("key3", "john");
			inputTopic.pipeInput("key4", "Zhang");
			inputTopic.pipeInput("key5", "Zhang");

			assertThat(outputTopic.readKeyValuesToList())
					.containsExactly(
							KeyValue.pair("hello", 1L),
							KeyValue.pair("world", 1L),
							KeyValue.pair("hello", 2L),
							KeyValue.pair("john", 1L),
							KeyValue.pair("zhang", 1L),
							KeyValue.pair("zhang", 2L)

					);
		}
	}

}
