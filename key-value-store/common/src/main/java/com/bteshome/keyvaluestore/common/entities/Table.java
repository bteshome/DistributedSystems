package com.bteshome.keyvaluestore.common.entities;

import com.bteshome.keyvaluestore.common.requests.TableCreateRequest;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serial;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Table implements Serializable {
    private String name;
    private int replicationFactor;
    private Map<Integer, Partition> partitions = new HashMap<>();
    @Serial
    private static final long serialVersionUID = 1L;

    public static Table toTable(TableCreateRequest request) {
        Table table = new Table();
        table.setName(request.getTableName());
        table.setReplicationFactor(request.getReplicationFactor());
        for (int partitionId = 1; partitionId <= request.getNumPartitions(); partitionId++) {
            table.getPartitions().put(partitionId, new Partition(partitionId));
        }
        return table;
    }

    public Table copy() {
        Table table = new Table();
        table.setName(this.name);
        table.setReplicationFactor(this.replicationFactor);
        for (Partition partition : this.partitions.values()) {
            table.partitions.put(partition.getId(), partition.copy());
        }
        return table;
    }
}
