package com.bteshome.keyvaluestore.metadata.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class StorageNode {
    private String id;
    private String host;
    private int port;
    private int jmxPort;
    private String rack;

    public static StorageNode toStorageNode(String messageString) {
        String[] parts = messageString.split(" ");
        String id = parts[1].split("=")[1];
        String host = parts[2].split("=")[1];
        int port = Integer.parseInt(parts[3].split("=")[1]);
        int jmxPort = Integer.parseInt(parts[4].split("=")[1]);
        String rack = parts[5].split("=")[1];

        return new StorageNode(
                id,
                host,
                port,
                jmxPort,
                rack
        );
    }

    public String toString() {
        return "StorageNode id=%s host=%s port=%s jmxPort=%s rack=%s".formatted(
                id, host, port, jmxPort, rack
        );
    }
}
