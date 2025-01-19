package com.bteshome.keyvaluestore.client;

import com.bteshome.consistenthashing.Ring;
import com.bteshome.keyvaluestore.common.ConfigKeys;
import com.bteshome.keyvaluestore.common.MetadataCache;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.IntStream;


@Component
public class KeyToPartitionMapper {
    private Ring createRing(int numPartitions) {
        int numVirtualPartitions = (Integer)MetadataCache.getInstance().getConfiguration(ConfigKeys.STORAGE_NODE_RING_NUM_VIRTUAL_PARTITIONS_KEY);
        List<String> partitions = IntStream.range(1, numPartitions + 1)
                .boxed()
                .map(p -> Integer.toString(p))
                .toList();
        Ring ring = new Ring(numVirtualPartitions);
        ring.addServers(partitions);
        return ring;
    }

    public int map(String table, String key) {
        int numPartitions = MetadataCache.getInstance().getNumPartitions(table);
        var ring = createRing(numPartitions);
        return Integer.parseInt(ring.getServer(key));
    }
}
