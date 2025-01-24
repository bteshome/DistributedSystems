package com.bteshome.keyvaluestore.storage.states;

import com.bteshome.keyvaluestore.common.LogPosition;

public record WALEntry(int leaderTerm,
                       long index,
                       String operation,
                       String key,
                       String value) {
    public static WALEntry fromString(String logEntry) {
        String[] parts = logEntry.split(" ");
        return new WALEntry(Integer.parseInt(parts[0]),
                            Long.parseLong(parts[1]),
                            parts[2],
                            parts[3],
                            parts.length == 5 ? parts[4] : "null");
    }

    @Override
    public String toString() {
        return "%s %s %s %s %s".formatted(leaderTerm,
                                          index,
                                          operation,
                                          key,
                                          value);
    }

    public int compareTo(LogPosition other) {
        if (this.leaderTerm() == other.leaderTerm())
            return Long.compare(this.index(), other.index());
        return Integer.compare(this.leaderTerm(), other.leaderTerm());
    }
}
