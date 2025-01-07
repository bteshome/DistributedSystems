package com.bteshome.keyvaluestore.common.entities;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Partition implements Serializable {
    private int id;
    private String leader;
    private List<String> replicas;
    private List<String> inSyncReplicas;
    @Serial
    private static final long serialVersionUID = 1L;

    public Partition(int id) {
        this.id = id;
        this.replicas = new ArrayList<>();
        this.inSyncReplicas = new ArrayList<>();
    }

    public Partition copy() {
        return new Partition(
                id,
                leader,
                replicas.stream().toList(),
                inSyncReplicas.stream().toList());
    }
}
