package com.bteshome.keyvaluestore.admindashboard.dto;

import com.bteshome.keyvaluestore.common.LogPosition;
import lombok.Getter;
import lombok.Setter;

import java.util.*;

@Getter
@Setter
public class TableCountDto {
    private List<PartitionCountDto> partitionCountDtos = new ArrayList<>();

    @Getter
    @Setter
    public static class PartitionCountDto {
        private int partitionId;
        private String leaderId;
        private Set<String> replicas;
        private Set<String> inSyncReplicas;
        private List<ReplicaCountDto> replicaCountDtos = new ArrayList<>();

        public int getPartitionCount() {
            return replicaCountDtos.stream()
                    .filter(ReplicaCountDto::isLeader)
                    .findFirst()
                    .orElseGet(ReplicaCountDto::new)
                    .getCount();
        }
    }

    @Getter
    @Setter
    public static class ReplicaCountDto {
        private String leaderId;
        private String replicaId;
        private int count;
        private LogPosition committedOffset;
        private LogPosition endOffset;
        private boolean active;

        public boolean isLeader() {
            return leaderId.equals(replicaId);
        }
    }

    public int getTableTotalCount() {
        return partitionCountDtos.stream()
                .mapToInt(PartitionCountDto::getPartitionCount)
                .sum();
    }
}
