package com.bteshome.keyvaluestore.storage.api.grpc.server;

import com.bteshome.keyvaluestore.storage.proto.WalFetchResponseProto;
import io.grpc.stub.StreamObserver;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class ReplicaRegistration {
    private String table;
    private int partition;
    private int maxNumRecords;
    private StreamObserver<WalFetchResponseProto> responseObserver;
}
