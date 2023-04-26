package com.spring.kafka.stream.string.code.config.processor;

import com.spring.kafka.stream.string.code.config.config.Constants;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.apache.kafka.streams.KeyValue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;


@Configuration
@EnableKafkaStreams
@Slf4j
public class KafkaProcessor {

    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final Serde<Long> LONG_SERDE = Serdes.Long();
    private static List<String> ignoreList = Arrays.asList("a","the","of","for","on","at","as","it","is","are","am","have","has","all",
                                                           "in","be","do","0","1","2","3","4","5","6","7","8","9","will","shall","and",
                                                           "to","you","I","he","she","we","they","he","him","her");
    private static Set<String> ignoreSet = new HashSet<>(ignoreList);

    @Bean  // too slow !!!
    public KStream<String , Long> kStream(StreamsBuilder streamsBuilder) {


        KStream<String, String> kstream = streamsBuilder.stream(Constants.INPUT_TOPIC,Consumed.with(STRING_SERDE, STRING_SERDE));

        KTable<String,Long> wordCounts  = kstream
                   //.mapValue  Hello world Hello John Hello Jessica -->hello world hello john hello jessica
                .mapValues(textLine -> textLine.replaceAll("[^a-zA-Z ]", "").toLowerCase())
                   //.flatMapValue  {null:hello, null:world null:hello null:john null:hello null: jessica
                .flatMapValues(textLine -> Arrays.asList(textLine.split("\\W+")))
                   //.selectKey  {hello:hello world:world hello:hello john:john hello:hello jessica: jessica
                   .map((key, word) -> new KeyValue<>(word, word))
                   //.groupByKey  {hello:hello hello:hello hello:hello world:world  john:john jessica:jessica
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .count();

              KStream<String, Long> result = wordCounts.toStream().filter((k,v) ->!ignoreSet.contains(k));

              result.to(Constants.OUTPUT_TOPIC,Produced.with(Serdes.String(), Serdes.Long()));

              return wordCounts.toStream();
    }
}
