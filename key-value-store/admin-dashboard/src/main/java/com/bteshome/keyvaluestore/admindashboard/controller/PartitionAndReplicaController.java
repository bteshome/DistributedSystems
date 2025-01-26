package com.bteshome.keyvaluestore.admindashboard.controller;

import com.bteshome.keyvaluestore.admindashboard.dto.PartitionAndReplicaListRequest;
import com.bteshome.keyvaluestore.admindashboard.service.TableService;
import com.bteshome.keyvaluestore.client.requests.ItemCountAndOffsetsRequest;
import com.bteshome.keyvaluestore.client.ItemReader;
import com.bteshome.keyvaluestore.client.responses.ItemCountAndOffsetsResponse;
import com.bteshome.keyvaluestore.common.LogPosition;
import com.bteshome.keyvaluestore.common.entities.Partition;
import com.bteshome.keyvaluestore.common.entities.Table;
import com.bteshome.keyvaluestore.common.requests.TableGetRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@Controller
@RequestMapping("/partitions-and-replicas")
@RequiredArgsConstructor
@Slf4j
public class PartitionAndReplicaController {
    @Autowired
    private TableService tableService;

    @Autowired
    ItemReader itemReader;

    @GetMapping("/")
    public String list(Model model) {
        PartitionAndReplicaListRequest request = new PartitionAndReplicaListRequest();
        request.setTable("table1");
        model.addAttribute("request", request);
        model.addAttribute("page", "partitions-and-replicas");
        return "partitions-and-replicas-list.html";
    }

    @PostMapping("/")
    public String list(@ModelAttribute("request") @RequestBody PartitionAndReplicaListRequest request, Model model) {
        try {
            TableGetRequest tableGetRequest = new TableGetRequest(request.getTable());
            Table table = tableService.getTable(tableGetRequest);
            Map<Integer, Integer> counts = null;
            Map<Integer, LogPosition> committedOffsets = null;
            Map<Integer, Map<String, LogPosition>> replicaEndOffsets = null;

            for (Partition partition : table.getPartitions().values()) {
                ItemCountAndOffsetsRequest itemCountAndOffsetsRequest = new ItemCountAndOffsetsRequest();
                itemCountAndOffsetsRequest.setTable(table.getName());
                itemCountAndOffsetsRequest.setPartition(partition.getId());
                ItemCountAndOffsetsResponse countAndOffsets = itemReader.getCountAndOffsets(itemCountAndOffsetsRequest);
                if (countAndOffsets != null) {
                    if (counts == null) {
                        counts = new HashMap<>();
                    }
                    if (committedOffsets == null) {
                        committedOffsets = new HashMap<>();
                    }
                    if (replicaEndOffsets == null) {
                        replicaEndOffsets = new HashMap<>();
                    }
                    counts.put(partition.getId(), countAndOffsets.getCount());
                    committedOffsets.put(partition.getId(), countAndOffsets.getCommitedOffset());
                    replicaEndOffsets.put(partition.getId(), countAndOffsets.getReplicaEndOffsets());
                }
            }

            model.addAttribute("request", request);
            model.addAttribute("partitions", table.getPartitions().values().stream().toList());
            model.addAttribute("counts", counts);
            if (counts != null)
                model.addAttribute("totalCount", counts.values().stream().mapToInt(v -> v).sum());
            model.addAttribute("committedOffsets", committedOffsets);
            model.addAttribute("replicaEndOffsets", replicaEndOffsets);
            model.addAttribute("page", "partitions-and-replicas");
            return "partitions-and-replicas-list.html";
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
            model.addAttribute("request", request);
            model.addAttribute("page", "partitions-and-replicas");
            return "partitions-and-replicas-list.html";
        }
    }
}
