package com.bteshome.keyvaluestore.common.entities;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Replica implements Serializable {
    private String nodeId;
    private String table;
    private int partition;
    @Serial
    private static final long serialVersionUID = 1L;

    @Override
    public String toString() {
        return "%s-%s-%s".formatted(table, partition, nodeId);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        Replica replica = (Replica) o;
        return partition == replica.partition && Objects.equals(nodeId, replica.nodeId) && Objects.equals(table, replica.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, table, partition);
    }
}
