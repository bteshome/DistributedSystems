package com.bteshome.keyvaluestore.storage.api.grpc;

import com.bteshome.keyvaluestore.storage.proto.LogPositionProto;
import com.bteshome.keyvaluestore.storage.proto.WalEntryProto;
import com.google.protobuf.ByteString;

import java.nio.ByteBuffer;

public class WalEntryProtoUtils {
    public static WalEntryProto fromByteBuffer(ByteBuffer buffer) {
        int term = buffer.getInt();
        long index = buffer.getLong();
        long timestamp = buffer.getLong();
        byte operationType = buffer.get();
        long expiryTime = buffer.getLong();
        byte keyLength = buffer.get();
        byte partitionKeyLength = buffer.get();
        short valueLength = buffer.getShort();
        short indexLength = buffer.getShort();

        byte[] key = new byte[keyLength];
        buffer.get(key);

        byte[] partitionKey = partitionKeyLength == 0 ? null : new byte[partitionKeyLength];
        if (partitionKeyLength > 0)
            buffer.get(partitionKey);

        byte[] value = valueLength == 0 ? null : new byte[valueLength];
        if (valueLength > 0)
            buffer.get(value);

        byte[] indexes = indexLength == 0 ? null : new byte[indexLength];
        if (indexLength > 0)
            buffer.get(indexes);

        WalEntryProto.Builder builder = WalEntryProto.newBuilder()
                .setTerm(term)
                .setIndex(index)
                .setTimestamp(timestamp)
                .setOperationType(operationType)
                .setExpiryTime(expiryTime)
                .setKeyLength(keyLength)
                .setPartitionKeyLength(partitionKeyLength)
                .setValueLength(valueLength)
                .setIndexLength(indexLength)
                .setKey(ByteString.copyFrom(key));

        if (partitionKey != null)
            builder = builder.setPartitionKey(ByteString.copyFrom(partitionKey));

        if (value != null)
            builder = builder.setValue(ByteString.copyFrom(value));

        if (indexes != null)
            builder = builder.setIndexes(ByteString.copyFrom(indexes));

        return builder.build();
    }

    public static boolean equals(WalEntryProto walEntry, int otherLeaderTerm, long otherIndex) {
        return compare(walEntry, otherLeaderTerm, otherIndex) == 0;
    }

    public static boolean equals(WalEntryProto walEntry, LogPositionProto other) {
        return compare(walEntry, other) == 0;
    }

    public static boolean isGreaterThan(WalEntryProto a, WalEntryProto b) {
        return compare(a, b) > 0;
    }

    public static boolean isGreaterThan(WalEntryProto walEntry, LogPositionProto other) {
        return compare(walEntry, other) > 0;
    }

    public static boolean isGreaterThan(WalEntryProto walEntry, int otherLeaderTerm, long otherIndex) {
        return compare(walEntry, otherLeaderTerm, otherIndex) > 0;
    }

    public static boolean isGreaterThanOrEquals(WalEntryProto a, WalEntryProto b) {
        return compare(a, b) >= 0;
    }

    public static boolean isGreaterThanOrEquals(WalEntryProto walEntry, LogPositionProto other) {
        return compare(walEntry, other) >= 0;
    }

    public static boolean isGreaterThanOrEquals(WalEntryProto walEntry, int otherLeaderTerm, long otherIndex) {
        return compare(walEntry, otherLeaderTerm, otherIndex) >= 0;
    }

    public static boolean isLessThan(WalEntryProto a, WalEntryProto b) {
        return compare(a, b) < 0;
    }

    public static boolean isLessThan(WalEntryProto walEntry, LogPositionProto other) {
        return compare(walEntry, other) < 0;
    }

    public static boolean isLessThan(WalEntryProto walEntry, int otherTerm, long otherIndex) {
        return compare(walEntry, otherTerm, otherIndex) < 0;
    }

    public static boolean isLessThanOrEquals(WalEntryProto a, WalEntryProto b) {
        return compare(a, b) <= 0;
    }

    public static boolean isLessThanOrEquals(WalEntryProto walEntry, LogPositionProto other) {
        return compare(walEntry, other) <= 0;
    }

    public static boolean isLessThanOrEquals(WalEntryProto walEntry, int otherTerm, long otherIndex) {
        return compare(walEntry, otherTerm, otherIndex) <= 0;
    }

    private static int compare(WalEntryProto walEntry, int otherTerm, long otherIndex) {
        return compare(walEntry.getTerm(), walEntry.getIndex(), otherTerm, otherIndex);
    }

    private static int compare(WalEntryProto walEntry, LogPositionProto position) {
        return compare(walEntry.getTerm(), walEntry.getIndex(), position.getTerm(), position.getIndex());
    }

    public static int compare(WalEntryProto a, WalEntryProto b) {
        return compare(a.getTerm(), a.getIndex(), b.getTerm(), b.getIndex());
    }

    private static int compare(int term1, long index1, int term2, long index2) {
        if (term1 == term2)
            return Long.compare(index1, index2);
        return Integer.compare(term1, term2);
    }

    public static LogPositionProto getOffset(WalEntryProto walEntry) {
        return LogPositionProto.newBuilder()
                .setTerm(walEntry.getTerm())
                .setIndex(walEntry.getIndex())
                .build();
    }
}
