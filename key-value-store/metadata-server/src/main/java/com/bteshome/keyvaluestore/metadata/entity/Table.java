package com.bteshome.keyvaluestore.metadata.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Table {
    private String name;
    private int numPartitions;

    public static Table toTable(String messageString) {
        String[] parts = messageString.split(" ");
        String tableName = parts[1].split("=")[1];
        int numPartitions = Integer.parseInt(parts[2].split("=")[1]);
        return new Table(tableName, numPartitions);
    }

    public String toString() {
        return "Table name=%s partitions=%s".formatted(name, numPartitions);
    }
}
