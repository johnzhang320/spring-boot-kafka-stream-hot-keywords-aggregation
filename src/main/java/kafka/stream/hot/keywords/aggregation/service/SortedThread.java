package kafka.stream.hot.keywords.aggregation.service;

import lombok.AllArgsConstructor;


@AllArgsConstructor
public class SortedThread implements Runnable{
    private final SharedBlockingQueue sharedBlockingQueue;

    @Override
    public void run() {
        while (true) {
            sharedBlockingQueue.sortAndRender();
        }
    }
}
