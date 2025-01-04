package com.bteshome.keyvaluestore.adminclient.dto;

import com.bteshome.keyvaluestore.adminclient.message.NotFoundResponse;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.nio.charset.StandardCharsets;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class Table {
    private String name;
    private int numPartitions;

    public static Table toTable(ByteString content) {
        String messageString = content.toString(StandardCharsets.UTF_8);

        if (messageString.equals(NotFoundResponse.VALUE)) {
            return null;
        }

        String[] parts = messageString.split(" ");
        String tableName = parts[0].split("=")[1];
        int numPartitions = Integer.parseInt(parts[1].split("=")[1]);
        return new Table(tableName, numPartitions);
    }
}
