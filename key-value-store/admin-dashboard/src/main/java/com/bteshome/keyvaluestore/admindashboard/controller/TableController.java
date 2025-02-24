package com.bteshome.keyvaluestore.admindashboard.controller;

import com.bteshome.keyvaluestore.admindashboard.dto.TableCreateDto;
import com.bteshome.keyvaluestore.admindashboard.service.TableService;
import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.common.entities.Table;
import com.bteshome.keyvaluestore.common.requests.TableCreateRequest;
import com.bteshome.keyvaluestore.common.requests.TableListRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Controller
@RequestMapping("/tables")
@RequiredArgsConstructor
@Slf4j
public class TableController {
    @Autowired
    private TableService tableService;

    @GetMapping("/create/")
    public String create(Model model) {
        TableCreateDto tableCreateDto = new TableCreateDto();
        tableCreateDto.setTableName("products");
        tableCreateDto.setNumPartitions(1);
        tableCreateDto.setReplicationFactor(1);
        tableCreateDto.setMinInSyncReplicas(1);
        tableCreateDto.setTimeToLiveEnabled(false);
        tableCreateDto.setTimeToLive(Duration.ofMinutes(5L));
        tableCreateDto.setIndexNames("category");
        model.addAttribute("table", tableCreateDto);
        model.addAttribute("page", "tables");
        return "tables-create.html";
    }

    @PostMapping("/create/")
    public String create(@ModelAttribute("table") @RequestBody TableCreateDto tableCreateDto, Model model) {
        try {
            tableCreateDto.validate(MetadataCache.getInstance().getConfigurations());
            TableCreateRequest table = new TableCreateRequest();
            table.setTableName(tableCreateDto.getTableName());
            table.setNumPartitions(tableCreateDto.getNumPartitions());
            table.setReplicationFactor(tableCreateDto.getReplicationFactor());
            table.setMinInSyncReplicas(tableCreateDto.getMinInSyncReplicas());
            if (tableCreateDto.isTimeToLiveEnabled())
                table.setTimeToLive(tableCreateDto.getTimeToLive());
            if (Strings.isNotBlank(tableCreateDto.getIndexNames()))
                table.setIndexNames(Arrays.stream(tableCreateDto.getIndexNames().split(",")).collect(Collectors.toSet()));
            tableService.createTable(table);
            return "redirect:/tables/";
        } catch (Exception e) {
            model.addAttribute("table", tableCreateDto);
            model.addAttribute("error", e.getMessage());
            model.addAttribute("page", "tables");
            return "tables-create.html";
        }
    }

    @GetMapping("/")
    public String list(Model model) {
        List<Table> tables = tableService.list(new TableListRequest());
        model.addAttribute("tables", tables);
        model.addAttribute("page", "tables");
        return "tables-list.html";
    }
}