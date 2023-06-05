package kafka.stream.hot.keywords.aggregation.service;

import kafka.stream.hot.keywords.aggregation.config.Constants;
import kafka.stream.hot.keywords.aggregation.model.KVCount;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class MessagingAgent {
    private static MessagingAgent messagingAgent=null;
    private static String startStopFlag="stop";

    private static String buffer="";

    public static MessagingAgent getMessagingAgent() {
        if (messagingAgent==null) {
            messagingAgent = new MessagingAgent();
        }
        return messagingAgent;
    }
    public static void setStart() {
        startStopFlag = "start";
        buffer="";

    }

    public static void setStop() {
        startStopFlag = "stop";
    }

    public static String getStartStopFlag() {
        return startStopFlag;
    }

    public static String getResult() {
        return buffer;
    }
    public static void setResult(String result) {
        buffer=result;
    }


}
