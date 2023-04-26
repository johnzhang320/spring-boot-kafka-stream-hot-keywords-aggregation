package com.spring.kafka.stream.string.code.config.service;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;


@AllArgsConstructor
public class SortedThread implements Runnable{
    private SharedBlockingQueue sharedBlockingQueue;
    @Override
    public void run() {
        while (true) {
            sharedBlockingQueue.sortAndRender();
        }
    }
}
