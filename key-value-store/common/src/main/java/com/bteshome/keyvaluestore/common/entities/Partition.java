package com.bteshome.keyvaluestore.common.entities;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
    public String toString() {
        return "%s-%s".formatted(tableName, id);
    }
}
