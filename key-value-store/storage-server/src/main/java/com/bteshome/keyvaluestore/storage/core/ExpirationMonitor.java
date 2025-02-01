package com.bteshome.keyvaluestore.storage.core;

import com.bteshome.keyvaluestore.common.ConfigKeys;
import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.common.Tuple;
import com.bteshome.keyvaluestore.storage.states.PartitionState;
import com.bteshome.keyvaluestore.storage.states.State;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class ExpirationMonitor {
    private ScheduledExecutorService executor = null;
    @Autowired
    State state;

    @PreDestroy
    public void close() {
        if (executor != null)
            executor.close();
    }

    public void schedule() {
        try {
            long interval = (Long)MetadataCache.getInstance().getConfiguration(ConfigKeys.EXPIRATION_MONITOR_INTERVAL_MS_KEY);
            executor = Executors.newSingleThreadScheduledExecutor();
            executor.scheduleAtFixedRate(this::checkStatus, interval, interval, TimeUnit.MILLISECONDS);
            log.info("Scheduled item expiration monitor. The interval is {} ms.", interval);
        } catch (Exception e) {
            log.error("Error scheduling item expiration monitor.", e);
        }
    }

    private void checkStatus() {
        log.trace("Item expiration monitor about to check if any items have expired.");

        try {
            String leaderNodeId = state.getNodeId();
            List<Tuple<String, Integer>> ownedPartitions = MetadataCache.getInstance().getOwnedPartitions(leaderNodeId);

            for (Tuple<String, Integer> ownedPartition : ownedPartitions) {
                String table = ownedPartition.first();
                int partition = ownedPartition.second();
                PartitionState partitionState = state.getPartitionState(table, partition);

                if (partitionState == null)
                    continue;

                partitionState.deleteExpiredItems();
            }
        } catch (Exception e) {
            log.error("Error checking item expiration.", e);
        }
    }
}
