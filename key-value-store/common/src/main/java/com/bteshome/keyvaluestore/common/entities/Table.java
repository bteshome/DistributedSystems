package com.bteshome.keyvaluestore.common.entities;

import com.bteshome.keyvaluestore.common.requests.TableCreateRequest;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Table implements Serializable {
    private String name;
    private int replicationFactor;
    private List<Partition> partitionList;
    @Serial
    private static final long serialVersionUID = 1L;

    public static Table toTable(TableCreateRequest request) {
        List<Partition> partitionList = IntStream.range(1, request.getNumPartitions() + 1)
                .boxed()
                .map(Partition::new)
                .toList();
        return new Table(request.getTableName(), request.getReplicationFactor(), partitionList);
    }

    public Table copy() {
        List<Partition> partitionList = this.partitionList.stream().map(Partition::copy).toList();
        return new Table(name, replicationFactor, partitionList);
    }
}
