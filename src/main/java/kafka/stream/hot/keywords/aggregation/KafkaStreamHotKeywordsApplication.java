package kafka.stream.hot.keywords.aggregation;

import kafka.stream.hot.keywords.aggregation.service.SharedBlockingQueue;
import kafka.stream.hot.keywords.aggregation.service.SortedThread;
import kafka.stream.hot.keywords.aggregation.service.WordsProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.File;

@SpringBootApplication
@RequiredArgsConstructor
public class KafkaStreamHotKeywordsApplication implements CommandLineRunner {
	private final SharedBlockingQueue sharedBlockingQueue;

	public static void main(String[] args) {
		SpringApplication.run(KafkaStreamHotKeywordsApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {

		Runnable runnable = new SortedThread(sharedBlockingQueue);
		Thread thread = new Thread(runnable);
		thread.run();
	}
}
