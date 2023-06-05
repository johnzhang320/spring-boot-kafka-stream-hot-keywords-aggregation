package kafka.stream.hot.keywords.aggregation.service;

import kafka.stream.hot.keywords.aggregation.config.Constants;
import kafka.stream.hot.keywords.aggregation.model.KVCount;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;


@AllArgsConstructor
@NoArgsConstructor
@Data
@Service
@Slf4j
public class SharedBlockingQueue {

    private  BlockingQueue<KVCount> sharedBlockingQueue = new ArrayBlockingQueue<>(Constants.QUEUE_SIZE);

    private  List<KVCount> sortedList = new ArrayList<>();
    private Map<String, Long> countMap = new HashMap<>();
    public void sortAndRender() {
        /**
         *   2 seconds of duration for thread really process
         *   sleep means the thread yield CPU time to others
         */
        try {
            Thread.sleep(4000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        synchronized (sortedList) {
            Boolean startFlag = MessagingAgent.getMessagingAgent().getStartStopFlag().equalsIgnoreCase("start");
          // System.out.println("\n\n--------startFlag-----------\n"+startFlag);
            if (!startFlag) {
                return;
            }
            /**
             *  Unique for current kvCounts by CountMap and only max count of KVCount be saved into CountMap
             */
            while (!sharedBlockingQueue.isEmpty()) {
                KVCount kvCount = sharedBlockingQueue.poll();
                String key = kvCount.getKey();
                if (key.isEmpty() || key == null || key == "") continue;
                Long value = kvCount.getCount();
                if (countMap.containsKey(key)) {
                    Long count = countMap.get(key);
                    if (count < value) {
                        countMap.put(key, value);
                    }
                } else {
                    countMap.put(key, value);
                }
                if (countMap.size() > Constants.MOST_OF_COUNT) break;
            }

            countMap.forEach((k, v) -> sortedList.add(new KVCount(k, v)));
            countMap = new HashMap<>();

            StringBuffer buffer = new StringBuffer();
            if (!sortedList.isEmpty()) {
                System.out.println("\n------------------Sorted Hot Keywords-------------------");
                /**
                 *  Sorting by Desc
                 */
                sortedList.sort((o1, o2) -> o2.getCount().compareTo(o1.getCount()));
                AtomicInteger sequence_of_unique_words = new AtomicInteger(1);
                for (KVCount kvCount : sortedList) {
                    System.out.println(sequence_of_unique_words.getAndIncrement() + " " + kvCount.getKey() + " ==> " + kvCount.getCount());
                    buffer.append(kvCount.getKey() + " ==> " + kvCount.getCount() + "\n");
                     MessagingAgent.getMessagingAgent().setResult(buffer.toString());
                }
                sortedList.clear();

            }
            MessagingAgent.getMessagingAgent().setStop();

        }
    }
}
