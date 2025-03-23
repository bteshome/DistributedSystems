package com.bteshome.keyvaluestore.storage.api.grpc.server;

import com.bteshome.keyvaluestore.storage.proto.WalFetchResponseProto;
import io.grpc.stub.StreamObserver;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Objects;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class ReplicaRegistration {
    private String replicaId;
    private String table;
    private int partition;
    private int maxNumRecords;
    private StreamObserver<WalFetchResponseProto> responseObserver;

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        ReplicaRegistration that = (ReplicaRegistration) o;
        return partition == that.partition && Objects.equals(replicaId, that.replicaId) && Objects.equals(table, that.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(replicaId, table, partition);
    }
}
