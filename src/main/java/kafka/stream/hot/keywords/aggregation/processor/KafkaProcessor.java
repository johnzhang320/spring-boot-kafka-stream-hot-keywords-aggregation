package kafka.stream.hot.keywords.aggregation.processor;

import kafka.stream.hot.keywords.aggregation.config.Constants;
import kafka.stream.hot.keywords.aggregation.model.KVCount;
import kafka.stream.hot.keywords.aggregation.service.MessagingAgent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.apache.kafka.streams.KeyValue;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;


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
    private Integer count=0;
    @Bean  // too slow !!!
    public KStream<String , Long> kStream(StreamsBuilder streamsBuilder) {

        KStream<String, String> kstream = streamsBuilder.stream(Constants.INPUT_TOPIC
                ,Consumed.with(STRING_SERDE, STRING_SERDE));
        Duration hoppingWindowSize = Duration.ofSeconds(4L);
        Duration advanceWindowSize = Duration.ofMillis(1000L);
        List<KeyValue<String,Long>> sortedList = new ArrayList<>();
        KStream<String,Long> wordCounts  = kstream
                   //.mapValue  Hello world Hello John Hello Jessica -->hello world hello john hello jessica
                .mapValues(textLine -> textLine.replaceAll("[^a-zA-Z ]", "").toLowerCase())
                   //.flatMapValue  {null:hello, null:world null:hello null:john null:hello null: jessica
                .flatMapValues(textLine -> Arrays.asList(textLine.split("\\W+")))
                   //.selectKey  {hello:hello world:world hello:hello john:john hello:hello jessica: jessica
                 .map((key, word) -> new KeyValue<>(word, word))
                   //.groupByKey  {hello:hello hello:hello hello:hello world:world  john:john jessica:jessica
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(TimeWindows.of(hoppingWindowSize).advanceBy(advanceWindowSize).grace(Duration.ofSeconds(1)))
                .count()
                .suppress(untilWindowCloses(unbounded()))
                .toStream()
                .map((key,value)-> KeyValue.pair(key.key(),value))
                .filter((k,v) ->v>2 && !ignoreSet.contains(k))
                //.peek((key,value)->sortedList.add(new KeyValue<>(key, value)));
                .peek((key,value)->log.info("Peek Suppressed Key="+key +", value="+value))
                .peek((key,value)->sortedList.add(new KeyValue<>(key,value)));

              //   System.out.println("\nUse below comment code to try to Sort Hot Keywords here but not succeed!");
              //   Bean initialize call this code and once hoping window running , below cade does not run !!!
              /*  if (!sortedList.isEmpty()) {
                    StringBuffer buffer = new StringBuffer();
                    sortedList.sort((o1, o2) -> o2.value.compareTo(o1.value));
                    int cnt=1;
                    for (KeyValue kvCount : sortedList) {
                        System.out.println((cnt++) + " " + kvCount.key + " ==> " + kvCount.value);
                        buffer.append(kvCount.key + " ==> " + kvCount.value + "\n");
                        MessagingAgent.getMessagingAgent().setResult(buffer.toString());
                    }
                    sortedList.clear();
                    MessagingAgent.getMessagingAgent().setStop();
                }*/

                
               wordCounts.to(Constants.OUTPUT_TOPIC,Produced.with(Serdes.String(), Serdes.Long()));

              return wordCounts;
    }
}
