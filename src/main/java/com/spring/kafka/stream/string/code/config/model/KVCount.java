package com.spring.kafka.stream.string.code.config.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class KVCount {
    private String key;
    private Long count;
}
