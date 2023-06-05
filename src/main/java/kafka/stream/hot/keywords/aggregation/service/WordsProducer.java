package kafka.stream.hot.keywords.aggregation.service;

import kafka.stream.hot.keywords.aggregation.config.Constants;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@EnableKafka
@RequiredArgsConstructor
public class WordsProducer {
    private final KafkaTemplate<String,String> kafkaTemplate;
    private final LoadFileToSend loadFileToSend;
    public void uploadTestFileSend() {
        String resume = loadFileToSend.readFileFromResource("resume.txt");
        kafkaTemplate.send(Constants.INPUT_TOPIC,resume);
    }
    public void uploadTestFileSend(String fileName) {
        String resume = loadFileToSend.readFileFromResource(fileName);
        System.out.println("File Read:"+resume);
        kafkaTemplate.send(Constants.INPUT_TOPIC,resume);
    }
}
