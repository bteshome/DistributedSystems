package com.bteshome.keyvaluestore.common.entities;

import com.bteshome.keyvaluestore.common.requests.StorageNodeJoinRequest;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serial;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class StorageNode implements Serializable {
    private String id;
    private String host;
    private int port;
    private int jmxPort;
    private String rack;
    private String storageDir;
    private StorageNodeStatus status;
    private Set<ReplicaAssignment> replicaAssignmentSet;
    @Serial
    private static final long serialVersionUID = 1L;

    public static StorageNode toStorageNode(StorageNodeJoinRequest request) {
        return new StorageNode(
                request.getId(),
                request.getHost(),
                request.getPort(),
                request.getJmxPort(),
                request.getRack(),
                request.getStorageDir(),
                StorageNodeStatus.INACTIVE,
                new HashSet<>()
        );
    }

    public StorageNode copy() {
        Set<ReplicaAssignment> replicaAssignmentSet = this.replicaAssignmentSet
                .stream()
                .map(ReplicaAssignment::copy)
                .collect(Collectors.toSet());
        return new StorageNode(
                id,
                host,
                port,
                jmxPort,
                rack,
                storageDir,
                status,
                replicaAssignmentSet
        );
    }

    public boolean isActive() {
        return status == StorageNodeStatus.ACTIVE;
    }
}
