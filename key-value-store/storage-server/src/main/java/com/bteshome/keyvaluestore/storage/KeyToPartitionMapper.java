package com.bteshome.keyvaluestore.storage;

import com.bteshome.consistenthashing.Ring;
import com.bteshome.keyvaluestore.common.ConfigKeys;
import com.bteshome.keyvaluestore.common.MetadataCache;

import java.util.List;
import java.util.stream.IntStream;

public class KeyToPartitionMapper {
    private static Ring createRing(int numPartitions) {
        List<String> partitions = IntStream.range(1, numPartitions + 1).boxed().map(p -> Integer.toString(p)).toList();
        int numOfVirtualNodes = (Integer) MetadataCache.getInstance().getConfiguration(ConfigKeys.STORAGE_NODE_RING_NUM_VIRTUAL_NODES_KEY);
        Ring ring = new Ring(numOfVirtualNodes);
        ring.addServers(partitions);
        return ring;
    }

    public static int map(String table, String key) {
        int numPartitions = MetadataCache.getInstance().getNumPartitions(table);
        var ring = createRing(numPartitions);
        return Integer.parseInt(ring.getServer(key));
    }
}
