package com.bteshome.keyvaluestore.storage.entities;

import com.bteshome.keyvaluestore.common.LogPosition;

import java.nio.ByteBuffer;

public record WALEntry(int leaderTerm,
                       long index,
                       long timestamp,
                       byte operation,
                       long expiryTime,
                       byte keyLength,
                       short valueLength,
                       short indexLength,
                       byte[] key,
                       byte[] value,
                       byte[] indexes) {
    public static WALEntry fromByteBuffer(ByteBuffer buffer) {
        int term = buffer.getInt();
        long index = buffer.getLong();
        long timestamp = buffer.getLong();
        byte operationType = buffer.get();
        long expiryTime = buffer.getLong();
        byte keyLength = buffer.get();
        short valueLength = buffer.getShort();
        short indexLength = buffer.getShort();

        byte[] key = new byte[keyLength];
        buffer.get(key);

        byte[] value = valueLength == 0 ? null : new byte[valueLength];
        if (valueLength > 0)
            buffer.get(value);

        byte[] indexes = indexLength == 0 ? null : new byte[indexLength];
        if (indexLength > 0)
            buffer.get(indexes);

        return new WALEntry(term,
                            index,
                            timestamp,
                            operationType,
                            expiryTime,
                            keyLength,
                            valueLength,
                            indexLength,
                            key,
                            value,
                            indexes);
    }

    public boolean equals(WALEntry other) {
        return compare(this, other) == 0;
    }

    public boolean equals(int otherLeaderTerm, long otherIndex) {
        return compare(this, otherLeaderTerm, otherIndex) == 0;
    }

    public boolean equals(LogPosition other) {
        return compare(this, other) == 0;
    }

    public boolean isGreaterThan(WALEntry other) {
        return compare(this, other) > 0;
    }

    public boolean isGreaterThan(LogPosition other) {
        return compare(this, other) > 0;
    }

    public boolean isGreaterThan(int otherLeaderTerm, long otherIndex) {
        return compare(this, otherLeaderTerm, otherIndex) > 0;
    }

    public boolean isGreaterThanOrEquals(WALEntry other) {
        return compare(this, other) >= 0;
    }

    public boolean isGreaterThanOrEquals(LogPosition other) {
        return compare(this, other) >= 0;
    }

    public boolean isGreaterThanOrEquals(int otherLeaderTerm, long otherIndex) {
        return compare(this, otherLeaderTerm, otherIndex) >= 0;
    }

    public boolean isLessThan(WALEntry other) {
        return compare(this, other) < 0;
    }

    public boolean isLessThan(LogPosition other) {
        return compare(this, other) < 0;
    }

    public boolean isLessThan(int otherLeaderTerm, long otherIndex) {
        return compare(this, otherLeaderTerm, otherIndex) < 0;
    }

    public boolean isLessThanOrEquals(WALEntry other) {
        return compare(this, other) <= 0;
    }

    public boolean isLessThanOrEquals(LogPosition other) {
        return compare(this, other) <= 0;
    }

    public boolean isLessThanOrEquals(int otherLeaderTerm, long otherIndex) {
        return compare(this, otherLeaderTerm, otherIndex) <= 0;
    }

    private static int compare(WALEntry walEntry, int otherLeaderTerm, long otherIndex) {
        return LogPosition.compare(walEntry.leaderTerm(), walEntry.index(), otherLeaderTerm, otherIndex);
    }

    private static int compare(WALEntry walEntry, LogPosition logPosition) {
        return LogPosition.compare(walEntry.leaderTerm(), walEntry.index(), logPosition.leaderTerm(), logPosition.index());
    }

    private static int compare(WALEntry walEntry1, WALEntry walEntry2) {
        return LogPosition.compare(walEntry1.leaderTerm(), walEntry1.index(), walEntry2.leaderTerm(), walEntry2.index());
    }

    public LogPosition getOffset() {
        return LogPosition.of(leaderTerm, index);
    }
}
