package com.bteshome.keyvaluestore.storage.api.grpc.server;

import com.bteshome.keyvaluestore.common.LogPosition;
import com.bteshome.keyvaluestore.storage.proto.WalFetchRequestProto;
import com.bteshome.keyvaluestore.storage.proto.WalFetchResponseProto;
import com.bteshome.keyvaluestore.storage.proto.WalServiceProtoGrpc;
import com.bteshome.keyvaluestore.storage.states.PartitionState;
import com.bteshome.keyvaluestore.storage.states.State;
import io.grpc.stub.StreamObserver;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

@Slf4j
public class WalService extends WalServiceProtoGrpc.WalServiceProtoImplBase {
    private final State state;
    private final Set<ReplicaRegistration> replicas;

    public WalService(State state) {
        this.state = state;
        this.replicas = new CopyOnWriteArraySet<>();
    }

    @Override
    public StreamObserver<WalFetchRequestProto> fetch(StreamObserver<WalFetchResponseProto> responseObserver) {
        return new StreamObserver<WalFetchRequestProto>() {
            private ReplicaRegistration replicaRegistration;

            @Override
            public void onNext(WalFetchRequestProto request) {
                PartitionState partitionState = state.getPartitionState(
                        request.getTable(),
                        request.getPartition());

                if (partitionState == null)
                    return;

                if (replicaRegistration == null) {
                    replicaRegistration = new ReplicaRegistration(
                            request.getId(),
                            request.getTable(),
                            request.getPartition(),
                            request.getMaxNumRecords(),
                            responseObserver
                    );
                    replicas.add(replicaRegistration);
                    log.info("Replica {} connected to leader gRPC WAL service for table {} partition {}.",
                            request.getId(),
                            request.getTable(),
                            request.getPartition());
                }

                LogPosition lastFetchOffset = LogPosition.of(
                        request.getLastFetchOffset().getTerm(),
                        request.getLastFetchOffset().getIndex());
                LogPosition replicaCommittedOffset = LogPosition.of(
                        request.getCommittedOffset().getTerm(),
                        request.getCommittedOffset().getIndex());

                partitionState.getOffsetState().setReplicaEndOffset(request.getId(), lastFetchOffset);
                partitionState.getOffsetState().setReplicaCommittedOffset(request.getId(), replicaCommittedOffset);
            }

            @Override
            public void onError(Throwable throwable) {
                log.error("Replica {} gRPC WAL service stream error for table {} partition {}.",
                        replicaRegistration.getReplicaId(),
                        replicaRegistration.getTable(),
                        replicaRegistration.getPartition());
            }

            @Override
            public void onCompleted() {
                replicas.remove(replicaRegistration);
                responseObserver.onCompleted();
                log.info("Replica {} gRPC WAL service stream completed for table {} partition {}.",
                        replicaRegistration.getReplicaId(),
                        replicaRegistration.getTable(),
                        replicaRegistration.getPartition());
            }
        };
    }

    public void send() {
        for (ReplicaRegistration replicaRegistration : replicas) {
            String replicaId = replicaRegistration.getReplicaId();
            String table = replicaRegistration.getTable();
            int partition = replicaRegistration.getPartition();
            int maxNumRecords = replicaRegistration.getMaxNumRecords();
            StreamObserver<WalFetchResponseProto> responseObserver = replicaRegistration.getResponseObserver();

            PartitionState partitionState = state.getPartitionState(
                    table,
                    partition);

            // TODO - how do we handle when a partition is reassigned?
            if (partitionState == null)
                return;

            LogPosition lastFetchOffset = partitionState.getOffsetState().getReplicaEndOffset(replicaId);
            LogPosition replicaCommittedOffset = partitionState.getOffsetState().getReplicaCommittedOffset(replicaId);

            WalFetchResponseProto response = partitionState.getLogEntriesProto(
                    lastFetchOffset,
                    replicaCommittedOffset,
                    maxNumRecords,
                    replicaId);

            if (!response.equals(WalFetchResponseProto.getDefaultInstance()))
                responseObserver.onNext(response);
        }
    }
}
