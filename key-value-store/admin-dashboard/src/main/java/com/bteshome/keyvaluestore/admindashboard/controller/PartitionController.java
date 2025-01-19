package com.bteshome.keyvaluestore.admindashboard.controller;

import com.bteshome.keyvaluestore.admindashboard.service.TableService;
import com.bteshome.keyvaluestore.client.requests.ItemCountRequest;
import com.bteshome.keyvaluestore.client.ItemReader;
import com.bteshome.keyvaluestore.common.entities.Partition;
import com.bteshome.keyvaluestore.common.entities.Table;
import com.bteshome.keyvaluestore.common.requests.TableListRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/partitions")
@RequiredArgsConstructor
@Slf4j
public class PartitionController {
    @Autowired
    private TableService tableService;

    @Autowired
    ItemReader itemReader;

    @GetMapping("/")
    public String list(Model model) {
        List<Table> tables = tableService.list(new TableListRequest());
        model.addAttribute("tables", tables);

        if (!tables.isEmpty()) {
            Map<String, Map<Integer, Integer>> partitionCounts = new HashMap<>();
            for (Table table : tables) {
                partitionCounts.put(table.getName(), new HashMap<>());
                for (Partition partition : table.getPartitions().values()) {
                    ItemCountRequest request = new ItemCountRequest();
                    request.setTable(table.getName());
                    request.setPartition(partition.getId());
                    int count = itemReader.count(request);
                    partitionCounts.get(table.getName()).put(partition.getId(), count);
                }
            }
            model.addAttribute("partitionCounts", partitionCounts);
        }

        model.addAttribute("page", "partitions");
        return "partitions-list.html";
    }
}
