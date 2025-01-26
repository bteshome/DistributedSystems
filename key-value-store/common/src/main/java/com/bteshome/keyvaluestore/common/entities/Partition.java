package com.bteshome.keyvaluestore.common.entities;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serial;
import java.io.Serializable;
import java.util.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Partition implements Serializable {
    private int id;
    private String tableName;
    private String leader;
    private int leaderTerm;
    private Set<String> replicas;
    private Set<String> inSyncReplicas;
    @Serial
    private static final long serialVersionUID = 1L;

    public Partition(String tableName, int id) {
        this.tableName = tableName;
        this.id = id;
        this.replicas = new HashSet<>();
        this.inSyncReplicas = new HashSet<>();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        Partition partition = (Partition) o;
        return id == partition.id && Objects.equals(tableName, partition.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, tableName);
    }

    @Override
    public String toString() {
        return "%s-%s".formatted(tableName, id);
    }

    public Partition copy() {
        Partition copy = new Partition(tableName, id);
        copy.setLeader(leader);
        copy.setLeaderTerm(leaderTerm);
        copy.setReplicas(new HashSet<>(replicas));
        copy.setInSyncReplicas(new HashSet<>(inSyncReplicas));
        return copy;
    }
}
