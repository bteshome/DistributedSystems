package com.bteshome.keyvaluestore.common.entities;

import com.bteshome.keyvaluestore.common.requests.TableCreateRequest;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serial;
import java.io.Serializable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Table implements Serializable {
    private String name;
    private int replicationFactor;
    private int minInSyncReplicas;
    private Map<Integer, Partition> partitions = new HashMap<>();
    private Duration timeToLive;
    @Serial
    private static final long serialVersionUID = 1L;

    public static Table toTable(TableCreateRequest request) {
        Table table = new Table();
        table.setName(request.getTableName());
        table.setReplicationFactor(request.getReplicationFactor());
        table.setMinInSyncReplicas(request.getMinInSyncReplicas());
        table.setTimeToLive(request.getTimeToLive());
        for (int partitionId = 1; partitionId <= request.getNumPartitions(); partitionId++)
            table.getPartitions().put(partitionId, new Partition(table.getName(), partitionId));
        return table;
    }

    public long getNumOfflinePartitions() {
        return partitions.values().stream().filter(p -> p.getLeader() == null).count();
    }

    public Table copy() {
        Table table = new Table();
        table.setName(this.getName());
        table.setReplicationFactor(this.getReplicationFactor());
        table.setMinInSyncReplicas(this.getMinInSyncReplicas());
        table.setTimeToLive(this.getTimeToLive());
        for (Map.Entry<Integer, Partition> entry : this.getPartitions().entrySet())
            table.getPartitions().put(entry.getKey(), entry.getValue().copy());
        return table;
    }
}
