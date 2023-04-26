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
    /**
     * @Bean here is using 'kStream' as bean default bean name because in KafkaDefaultBeanConfigure class, we use
     *   @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME) configure the ksSream as
     *   default bean, therefore never change method name kStream(),
     *   Once I change kStream() to other name such as processor(), error message , change back kStream()
     *   and mvn clean install, same error message again and again, dumb spring kStream !
     * @param streamsBuilder object is injected automatically by spring
     * @return KStream<String,Long>
     */
/*
    TimeWindowedSerializer<String> windowedSerializer = new TimeWindowedSerializer<>(new StringSerializer());
    TimeWindowedDeserializer<String> windowedDeserializer = new TimeWindowedDeserializer<>(new StringDeserializer());
    Serde<Windowed<String>> windowedSerde = Serdes.serdeFrom(windowedSerializer, windowedDeserializer);
*/

    private static List<String> ignoreList = Arrays.asList("a","the","of","for","on","at","as","it","is","are","am","have","has","all",
                                                         "in","be","do","0","1","2","3","4","5","6","7","8","9","will","shall","and","to","you","I","he","she","we","they","he","him","her");
    private static Set<String> ignoreSet = new HashSet<>(ignoreList);
    Predicate<String> ignoreWords = x ->!ignoreSet.contains(x);
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
                //tumpling window
               // KTable<Windowed<String>, Long> wordCounts = kGroupedStream
               //     .windowedBy(TimeWindows.of(Duration.ofMillis(1000)))
               //     .count();

     /*   KTable<String, KeyValueCount> aggregate  =  kGroupedStream.aggregate(
                new Initializer<KeyValueCount>() {
                    @Override
                    public KeyValueCount apply() {
                        return new KeyValueCount("", 0L);
                    }
                }, new Aggregator<String, String, KeyValueCount>() {
                    @Override
                    public KeyValueCount apply(String key, String value, KeyValueCount keyValueCount) {
                        Long currentCount = keyValueCount.getCount()+1;
                        return new KeyValueCount(key,currentCount);
                    }
                }, Materialized.with(Serdes.String(), new KayValueCountSerde()));

                KStream<String, Long> wordCount =  aggregate.toStream().map((k, v) -> new KeyValue<>(k, v.getCount()));
*/             KStream<String, Long> result = wordCounts.toStream().filter((k,v) ->!ignoreSet.contains(k));

              result.to(Constants.OUTPUT_TOPIC,Produced.with(Serdes.String(), Serdes.Long()));

              return null;
    }

/*
    @Bean
    public KStream<String, String> kStream(StreamsBuilder streamsBuilder) {
        KStream<String, String> source = streamsBuilder.stream(Constants.INPUT_TOPIC);
        KStream<String, String> transformed = source.mapValues(value -> value.toUpperCase());
        transformed.to(Constants.OUTPUT_TOPIC);
        return transformed;
    }
*/
}
