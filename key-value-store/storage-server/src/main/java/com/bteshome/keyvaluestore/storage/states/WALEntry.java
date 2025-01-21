package com.bteshome.keyvaluestore.storage.states;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@AllArgsConstructor
@Setter
@Getter
public class WALEntry {
    private long index;
    private int leaderTerm;
    private String operation;
    private String key;
    private String value;

    public static WALEntry fromString(String logEntry) {
        String[] parts = logEntry.split(" ");
        return new WALEntry(
                Long.parseLong(parts[0]),
                Integer.parseInt(parts[1]),
                parts[2],
                parts[3],
                parts.length == 5 ? parts[4] : "null");
    }

    @Override
    public String toString() {
        return "%s %s %s %s %s".formatted(index, leaderTerm, operation, key, value);
    }
}
