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
public class ReplicaAssignment implements Serializable {
    private String tableName;
    private int partitionIid;
    @Serial
    private static final long serialVersionUID = 1L;

    public ReplicaAssignment copy() {
        return new ReplicaAssignment(tableName, partitionIid);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        ReplicaAssignment that = (ReplicaAssignment) o;
        return partitionIid == that.partitionIid && Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, partitionIid);
    }
}
