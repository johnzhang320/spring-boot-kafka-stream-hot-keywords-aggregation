package com.spring.kafka.stream.string.code.config.config;

public interface Constants {
   public final static String INPUT_TOPIC="input-topic";
   public final static String  OUTPUT_TOPIC="output-topic";

   public final static String  OUTPUT_AGGR_TOPIC="output-aggr_topic";
   public final static String CLIENT_ID_CONFIG= "wordcount-client";
   public final static String BOOTSTRAP_SERVER="localhost:9092";

   public final static String CONSUMER_GROUP_ID="my_group";

   public final static String APPLICATION_CONFIG_ID="kStream_config_test";

   public final static Integer QUEUE_SIZE=10000;

   public final static Integer MOST_OF_COUNT=1000;


}
