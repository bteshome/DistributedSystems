package com.bteshome.keyvaluestore.storage.api.grpc.client;

import com.bteshome.keyvaluestore.common.JavaSerDe;
import com.bteshome.keyvaluestore.common.LogPosition;
import com.bteshome.keyvaluestore.common.Tuple;
import com.bteshome.keyvaluestore.storage.entities.DataSnapshot;
import com.bteshome.keyvaluestore.storage.proto.*;
import com.bteshome.keyvaluestore.storage.proto.WalServiceProtoGrpc.WalServiceProtoStub;
import com.bteshome.keyvaluestore.storage.states.PartitionState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;

@Slf4j
public class WalFetcherGrpc {
    private String nodeId;
    private String table;
    private int partition;
    private int maxNumRecords;
    private PartitionState partitionState;
    private ManagedChannel client;
    private StreamObserver<WalFetchRequestProto> requestObserver;

    public WalFetcherGrpc(PartitionState partitionState,
                          String nodeId,
                          String table,
                          int partition,
                          Tuple<String, Integer> leaderEndpoint,
                          int maxNumRecords) {
        if (leaderEndpoint == null) {
            log.trace("No leader for table '{}' partition '{}'. Skipping scheduling fetch.", table, partition);
            return;
        }

        this.partitionState = partitionState;
        this.nodeId = nodeId;
        this.table = table;
        this.partition = partition;
        this.maxNumRecords = maxNumRecords;

        String leaderHost = leaderEndpoint.first();
        int leaderPort = leaderEndpoint.second();

        this.client = ManagedChannelBuilder
                .forAddress(leaderHost, leaderPort)
                .usePlaintext()
                .build();
    }

    public void start() {
        WalServiceProtoStub stub = WalServiceProtoGrpc.newStub(client);

        this.requestObserver = stub.fetch(new StreamObserver<WalFetchResponseProto>() {
            @Override
            public void onNext(WalFetchResponseProto response) {
                walFetched(response);
                sendOffsets();
            }

            @Override
            public void onError(Throwable throwable) {
                log.error("gRPC WAL service leader stream error for table {} partition {}: {}.",
                        table,
                        partition,
                        throwable.getMessage(),
                        throwable);
            }

            @Override
            public void onCompleted() {
                log.error("gRPC WAL service leader stream completed for table {} partition {}: {}.",
                        table,
                        partition);
            }
        });

        sendOffsets();
    }

    public void stop() {
        try {
            if (requestObserver != null)
                requestObserver.onCompleted();
            if (client != null)
                client.shutdown();
            log.info("Shut down WAL fetcher for table {} partition {}.", table, partition);
        } catch (Exception e) {
            log.error("Error shutting down WAL fetcher for table {} partition {}.", table, partition, e);
        }
    }

    private void sendOffsets() {
        LogPosition lastFetchOffset = partitionState.getOffsetState().getEndOffset();
        LogPosition committedOffset = partitionState.getOffsetState().getCommittedOffset();

        LogPositionProto lastFetchOffsetProto = LogPositionProto.newBuilder()
                .setTerm(lastFetchOffset.leaderTerm())
                .setIndex(lastFetchOffset.index())
                .build();
        LogPositionProto committedOffsetProto = LogPositionProto.newBuilder()
                .setTerm(committedOffset.leaderTerm())
                .setIndex(committedOffset.index())
                .build();

        WalFetchRequestProto request = WalFetchRequestProto.newBuilder()
                .setId(nodeId)
                .setTable(table)
                .setPartition(partition)
                .setLastFetchOffset(lastFetchOffsetProto)
                .setCommittedOffset(committedOffsetProto)
                .setMaxNumRecords(maxNumRecords)
                .build();

        requestObserver.onNext(request);
    }

    private void walFetched(WalFetchResponseProto response) {
        if (response.equals(WalFetchResponseProto.getDefaultInstance()))
            return;

        if (response.getHttpStatusCode() == HttpStatus.INTERNAL_SERVER_ERROR.value() || response.getHttpStatusCode() == HttpStatus.BAD_REQUEST.value()) {
            // TODO - change to error
            log.trace("Error fetching WAL for table '{}' partition '{}'. Http status: {}, error: {}.",
                    table,
                    partition,
                    response.getHttpStatusCode(),
                    response.getErrorMessage());
            return;
        }

        if (response.getHttpStatusCode() == HttpStatus.CONFLICT.value()) {
            LogPosition truncateToOffset = LogPosition.of(
                    response.getTruncateToOffset().getTerm(),
                    response.getTruncateToOffset().getIndex());
            log.info("Received a truncate request from the new leader for table '{}' partition '{}'. Truncating to offset '{}'.",
                    table,
                    partition,
                    truncateToOffset);
            partitionState.getWal().truncateToBeforeInclusive(truncateToOffset);
            return;
        }

        if (response.getHttpStatusCode() == HttpStatus.OK.value()) {
            LogPosition commitedOffset = response.getCommitedOffset().equals(LogPositionProto.getDefaultInstance()) ?
                    LogPosition.ZERO :
                    LogPosition.of(response.getCommitedOffset().getTerm(), response.getCommitedOffset().getIndex());

            if (response.getPayloadType().equals(WalFetchPayloadTypeProto.LOG)) {
                partitionState.appendLogEntriesProto(
                        response.getEntriesList(),
                        commitedOffset);

                log.trace("Fetched WAL for table '{}' partition '{}'. entries size={}, commited offset={}.",
                        table,
                        partition,
                        response.getEntriesList().size(),
                        commitedOffset);
            } else if (response.getPayloadType().equals(WalFetchPayloadTypeProto.SNAPSHOT)) {
                byte[] dataSnapshotBytes = response.getDataSnapshotBytes().isEmpty() ?
                        null :
                        response.getDataSnapshotBytes().toByteArray();
                DataSnapshot dataSnapshot = JavaSerDe.deserialize(dataSnapshotBytes);
                partitionState.applyDataSnapshot(dataSnapshot);

                log.trace("Fetched data snapshot for table '{}' partition '{}', last snapshot committed offset={}.",
                        table,
                        partition,
                        dataSnapshot.getLastCommittedOffset());
            } else {
                partitionState.getOffsetState().setCommittedOffset(commitedOffset);
            }
        }
    }
}
