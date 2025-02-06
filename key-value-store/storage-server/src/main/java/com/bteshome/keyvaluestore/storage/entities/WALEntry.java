package com.bteshome.keyvaluestore.storage.entities;

import com.bteshome.keyvaluestore.common.LogPosition;

import java.time.Instant;

public record WALEntry(int leaderTerm,
                       long index,
                       long timestamp,
                       String operation,
                       String key,
                       String value,
                       Instant expiryTime) {
    public static WALEntry fromString(String logEntry) {
        String[] parts = logEntry.split(" ");
        return new WALEntry(Integer.parseInt(parts[0]),
                            Long.parseLong(parts[1]),
                            Long.parseLong(parts[2]),
                            parts[3],
                            parts[4],
                            parts.length > 5 ? parts[5] : null,
                            parts.length > 6 ? Instant.parse(parts[6]) : null);
    }

    @Override
    public String toString() {
        return "%s %s %s %s %s %s %s".formatted(leaderTerm,
                                             index,
                                             timestamp,
                                             operation,
                                             key,
                                             value != null ? value : "",
                                             expiryTime != null ? expiryTime.toString() : "");
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

    public LogPosition getPosition() {
        return LogPosition.of(leaderTerm, index);
    }
}
