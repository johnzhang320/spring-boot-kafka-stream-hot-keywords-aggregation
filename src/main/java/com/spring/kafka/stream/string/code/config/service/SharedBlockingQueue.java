package com.spring.kafka.stream.string.code.config.service;

import com.spring.kafka.stream.string.code.config.config.Constants;
import com.spring.kafka.stream.string.code.config.model.KVCount;
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
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        synchronized (sortedList) {
            countMap.clear();
            /**
             *  Unique for current kvCounts by CountMap and only max count of KVCount be saved into CountMap
             */
            while (!sharedBlockingQueue.isEmpty()) {
                KVCount kvCount = sharedBlockingQueue.poll();
                String key = kvCount.getKey();
                if (key.isEmpty() || key==null || key=="") continue;
                Long value = kvCount.getCount();
                if (countMap.containsKey(key)) {
                    Long count = countMap.get(key);
                    if (count<value) {
                        countMap.put(key,value);
                    }
                } else {
                    countMap.put(key,value);
                }
                if (countMap.size()>Constants.MOST_OF_COUNT) break;
             }

            countMap.forEach((k,v)->sortedList.add(new KVCount(k,v)));

            if (!sortedList.isEmpty()) {
                System.out.println("\n------------------Most Often Used Words-------------------");
                /**
                 *  Sorting by Desc
                 */
                sortedList.sort((o1,o2)->o2.getCount().compareTo(o1.getCount()));
                AtomicInteger sequence_of_unique_words= new AtomicInteger(1);
                sortedList.forEach(kvCount-> System.out.println(sequence_of_unique_words.getAndIncrement()+" "+kvCount.getKey()+" ==> "+kvCount.getCount()));
             }
            sortedList.clear();
        }
    }
}
