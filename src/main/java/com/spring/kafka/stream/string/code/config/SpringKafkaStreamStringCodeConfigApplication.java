package com.spring.kafka.stream.string.code.config;

import com.spring.kafka.stream.string.code.config.service.SharedBlockingQueue;
import com.spring.kafka.stream.string.code.config.service.SortedThread;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@RequiredArgsConstructor
public class SpringKafkaStreamStringCodeConfigApplication implements CommandLineRunner {
	private final  SharedBlockingQueue sharedBlockingQueue;
	public static void main(String[] args) {
		SpringApplication.run(SpringKafkaStreamStringCodeConfigApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		Runnable runnable = new SortedThread(sharedBlockingQueue);
		Thread thread = new Thread(runnable);
		thread.run();
	}
}
