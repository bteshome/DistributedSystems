package com.bteshome.keyvaluestore.admindashboard.controller;

import com.bteshome.keyvaluestore.admindashboard.service.TableService;
import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.common.entities.Table;
import com.bteshome.keyvaluestore.common.requests.TableCreateRequest;
import com.bteshome.keyvaluestore.common.requests.TableListRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Controller
@RequestMapping("/tables")
@RequiredArgsConstructor
@Slf4j
public class TableController {
    @Autowired
    private TableService tableService;

    @GetMapping("/create/")
    public String create(Model model) {
        TableCreateRequest table = new TableCreateRequest();
        table.setTableName("table1");
        table.setNumPartitions(1);
        table.setReplicationFactor(1);
        table.setMinInSyncReplicas(1);
        model.addAttribute("table", table);
        model.addAttribute("page", "tables");
        return "tables-create.html";
    }

    @PostMapping("/create/")
    public String create(@ModelAttribute("table") @RequestBody TableCreateRequest table, Model model) {
        try {
            table.validate(MetadataCache.getInstance().getConfigurations());
            tableService.createTable(table);
            return "redirect:/tables/";
        } catch (Exception e) {
            model.addAttribute("table", table);
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