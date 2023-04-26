package com.spring.kafka.stream.string.code.config.Serde;

import com.spring.kafka.stream.string.code.config.model.KVCount;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class KayValueCountSerde implements Serde<KVCount> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serde.super.configure(configs, isKey);
    }

    @Override
    public void close() {
        Serde.super.close();
    }

    @Override
    public Serializer<KVCount> serializer() {
        return new Serializer<KVCount>() {
            @Override
            public byte[] serialize(String string, KVCount KVCount) {
                String str= KVCount.getKey()+","+ KVCount.getCount();
                return str.getBytes();
            }
        };
    }

    @Override
    public Deserializer<KVCount> deserializer() {

        return new Deserializer<KVCount>() {
            @Override
            public KVCount deserialize(String string, byte[] bytes) {
                String str = new String(bytes);
                String words[] = str.split(",");
                return new KVCount(words[0],Long.valueOf(words[1]));
            }
        };
    }
}
