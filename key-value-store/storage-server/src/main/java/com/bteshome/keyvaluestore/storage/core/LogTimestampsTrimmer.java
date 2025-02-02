package com.bteshome.keyvaluestore.storage.core;

import com.bteshome.keyvaluestore.common.ConfigKeys;
import com.bteshome.keyvaluestore.common.LogPosition;
import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.common.Tuple;
import com.bteshome.keyvaluestore.common.entities.Replica;
import com.bteshome.keyvaluestore.storage.states.PartitionState;
import com.bteshome.keyvaluestore.storage.states.State;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class LogTimestampsTrimmer {
    private ScheduledExecutorService executor = null;
    @Autowired
    State state;
    @Autowired
    ISRSynchronizer isrSynchronizer;

    @PreDestroy
    public void close() {
        if (executor != null)
            executor.close();
    }

    public void schedule() {
        try {
            // TODO - should be configurable?
            long interval = Duration.ofMinutes(15).toMillis();
            executor = Executors.newSingleThreadScheduledExecutor();
            executor.scheduleAtFixedRate(this::checkStatus, interval, interval, TimeUnit.MILLISECONDS);
            log.info("Scheduled log timestamps trimmer. The interval is {} ms.", interval);
        } catch (Exception e) {
            log.error("Error scheduling log timestamps trimmer.", e);
        }
    }

    private void checkStatus() {
        log.trace("Log timestamps trimmer about to trim the log timestamps.");

        try {
            String leaderNodeId = state.getNodeId();
            List<Tuple<String, Integer>> ownedPartitions = MetadataCache.getInstance().getOwnedPartitions(leaderNodeId);

            for (Tuple<String, Integer> ownedPartition : ownedPartitions) {
                String table = ownedPartition.first();
                int partition = ownedPartition.second();
                PartitionState partitionState = state.getPartitionState(table, partition);

                if (partitionState == null)
                    continue;

                partitionState.getOffsetState().trimLogTimestamps();
            }
        } catch (Exception e) {
            log.error("Error trimming log timestamps.", e);
        }
    }
}
