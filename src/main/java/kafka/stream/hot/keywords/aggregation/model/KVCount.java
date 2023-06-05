package kafka.stream.hot.keywords.aggregation.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class KVCount  {
    private String key;
    private Long count;

}
