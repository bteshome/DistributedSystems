package com.bteshome.keyvaluestore.storage;

import com.bteshome.consistenthashing.Ring;
import com.bteshome.keyvaluestore.common.ConfigKeys;
import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.storage.common.StorageServerException;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.util.AutoCloseableLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.IntStream;

@Component
@Slf4j
public class Store {
    private Map<String, Map<Integer, Map<String, String>>> state;
    private Map<String, Map<Integer, WAL>> wal;
    private ReentrantReadWriteLock lock;

    @Autowired
    ReplicationState replicationState;

    @Autowired
    StorageSettings storageSettings;

    @PostConstruct
    public void init() {
        state = new ConcurrentHashMap<>();
        wal = new ConcurrentHashMap<>();
        lock = new ReentrantReadWriteLock(true);
    }

    private AutoCloseableLock readLock() { return AutoCloseableLock.acquire(lock.readLock()); }

    private AutoCloseableLock writeLock() { return AutoCloseableLock.acquire(lock.writeLock()); }

    private Ring createRing(int numPartitions) {
        List<String> partitions = IntStream.range(1, numPartitions + 1).boxed().map( p -> Integer.toString(p)).toList();
        int numOfVirtualNodes = (Integer)MetadataCache.getInstance().getConfiguration(ConfigKeys.STORAGE_NODE_RING_NUM_VIRTUAL_NODES_KEY);
        Ring ring = new Ring(numOfVirtualNodes);
        ring.addServers(partitions);
        return ring;
    }

    private int mapToPartition(String table, String key) {
        int numPartitions = MetadataCache.getInstance().getNumPartitions(table);
        var ring = createRing(numPartitions);
        return Integer.parseInt(ring.getServer(key));
    }

    public ResponseEntity<?> put(String table, String key, String value) {
        try (AutoCloseableLock l = writeLock()) {
            int partition = mapToPartition(table, key);
            String thisNodeId = replicationState.getId();

            if (!MetadataCache.getInstance().getLeaderNodeId(table, partition).equals(thisNodeId)) {
                String errorMessage = "Not the leader for table '{}' partition '{}'.".formatted(table, partition);
                return ResponseEntity.status(HttpStatus.BAD_GATEWAY).body(errorMessage);
            }

            if (!wal.containsKey(table)) {
                wal.put(table, new ConcurrentHashMap<>());
            }
            if (!wal.get(table).containsKey(partition)) {
                WAL walEntry = new WAL(storageSettings.getStorageDir(), table, partition);
                wal.get(table).put(partition, walEntry);
            }
            if (!state.containsKey(table)) {
                state.put(table, new ConcurrentHashMap<>());
            }
            if (!state.get(table).containsKey(partition)) {
                state.get(table).put(partition, new ConcurrentHashMap<>());
            }

            wal.get(table).get(partition).appendLog("PUT", key, value);
            state.get(table).get(partition).put(key, value);
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            log.error("Error writing to WAL.", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error writing to WAL.");
        }
    }

    public ResponseEntity<?> get(String table, String key) {
        try (AutoCloseableLock l = readLock()) {
            int partition = mapToPartition(table, key);
            String thisNodeId = replicationState.getId();

            if (!MetadataCache.getInstance().getLeaderNodeId(table, partition).equals(thisNodeId)) {
                String errorMessage = "Not the leader for table '{}' partition '{}'.".formatted(table, partition);
                return ResponseEntity.status(HttpStatus.BAD_GATEWAY).body(errorMessage);
            }

            if (!state.containsKey(table)) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
            }
            if (!state.get(table).containsKey(partition)) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
            }

            return ResponseEntity.ok(state.get(table).get(partition).get(key));
        }
    }
}
