package com.spring.kafka.stream.string.code.config.service;


import com.spring.kafka.stream.string.code.config.config.Constants;
import com.spring.kafka.stream.string.code.config.model.KVCount;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.kstream.Windowed;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class WordCountConsumer {
    private final SharedBlockingQueue sharedBlockingQueue;
    @KafkaListener(topics = Constants.OUTPUT_TOPIC, groupId=Constants.CONSUMER_GROUP_ID)
    public void consume(ConsumerRecord<String, Long> record) {
        /**
         *  Add to BlackingQueue under the condition
         */
       if (sharedBlockingQueue.getSharedBlockingQueue().size()<Constants.QUEUE_SIZE) {
           if (sharedBlockingQueue.getSharedBlockingQueue().size() > Constants.QUEUE_SIZE - Constants.QUEUE_SIZE * 0.1) {
               if (record.value() > 1) {
                   sharedBlockingQueue.getSharedBlockingQueue().add(new KVCount(record.key(), record.value()));
               }
           } else {
               sharedBlockingQueue.getSharedBlockingQueue().add(new KVCount(record.key(), record.value()));
           }
       }
     }
}
