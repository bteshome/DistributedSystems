package com.bteshome.keyvaluestore.admindashboard.controller;

import com.bteshome.keyvaluestore.admindashboard.dto.PartitionListRequest;
import com.bteshome.keyvaluestore.admindashboard.dto.TableCountDto;
import com.bteshome.keyvaluestore.admindashboard.service.TableService;
import com.bteshome.keyvaluestore.client.readers.CountAndOffsetReader;
import com.bteshome.keyvaluestore.client.requests.ItemCountAndOffsetsRequest;
import com.bteshome.keyvaluestore.client.responses.ItemCountAndOffsetsResponse;
import com.bteshome.keyvaluestore.common.LogPosition;
import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.common.entities.Partition;
import com.bteshome.keyvaluestore.common.entities.Table;
import com.bteshome.keyvaluestore.common.requests.TableGetRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

@Controller
@RequestMapping("/partitions-and-replicas")
@RequiredArgsConstructor
@Slf4j
public class PartitionsAndReplicasController {
    @Autowired
    private TableService tableService;

    @Autowired
    CountAndOffsetReader countAndOffsetReader;

    @GetMapping("/")
    public String list(Model model) {
        PartitionListRequest request = new PartitionListRequest();
        request.setTable("table1");
        model.addAttribute("request", request);
        model.addAttribute("page", "partitions-and-replicas");
        return "partitions-and-replicas.html";
    }

    @PostMapping("/")
    public String list(@ModelAttribute("request") @RequestBody PartitionListRequest request, Model model) {
        try {
            TableGetRequest tableGetRequest = new TableGetRequest(request.getTable());
            Table table = tableService.getTable(tableGetRequest);
            TableCountDto tableCountDto = new TableCountDto();

            for (Partition partition : table.getPartitions().values()) {
                ItemCountAndOffsetsRequest itemCountAndOffsetsRequest = new ItemCountAndOffsetsRequest();
                itemCountAndOffsetsRequest.setTable(table.getName());
                itemCountAndOffsetsRequest.setPartition(partition.getId());

                TableCountDto.PartitionCountDto partitionCountDto = new TableCountDto.PartitionCountDto();
                partitionCountDto.setPartitionId(partition.getId());
                partitionCountDto.setLeaderId(partition.getLeader());
                partitionCountDto.setReplicas(partition.getReplicas());
                partitionCountDto.setInSyncReplicas(partition.getInSyncReplicas());
                tableCountDto.getPartitionCountDtos().add(partitionCountDto);

                for (String replicaId : partition.getReplicas()) {
                    String endpoint = MetadataCache.getInstance().getEndpoint(replicaId);
                    ItemCountAndOffsetsResponse countAndOffsets = countAndOffsetReader.getCountAndOffsets(itemCountAndOffsetsRequest, endpoint);
                    TableCountDto.ReplicaCountDto replicaCountDto = new TableCountDto.ReplicaCountDto();
                    replicaCountDto.setReplicaId(replicaId);
                    replicaCountDto.setLeaderId(partition.getLeader());

                    if (countAndOffsets != null) {
                        replicaCountDto.setCount(countAndOffsets.getCount());
                        replicaCountDto.setCommittedOffset(countAndOffsets.getCommitedOffset());
                        replicaCountDto.setEndOffset(countAndOffsets.getEndOffset());
                        replicaCountDto.setActive(true);
                    } else {
                        replicaCountDto.setCount(0);
                        replicaCountDto.setCommittedOffset(LogPosition.ZERO);
                        replicaCountDto.setEndOffset(LogPosition.ZERO);
                        replicaCountDto.setActive(false);
                    }
                    partitionCountDto.getReplicaCountDtos().add(replicaCountDto);
                }
            }

            model.addAttribute("request", request);
            model.addAttribute("tableCountDto", tableCountDto);
            model.addAttribute("page", "partitions-and-replicas");
            return "partitions-and-replicas.html";
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
            model.addAttribute("request", request);
            model.addAttribute("page", "partitions-and-replicas");
            return "partitions-and-replicas.html";
        }
    }
}
