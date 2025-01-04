package com.bteshome.keyvaluestore.adminclient.controller;

import com.bteshome.keyvaluestore.adminclient.dto.Table;
import com.bteshome.keyvaluestore.adminclient.service.TableService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/tables")
@RequiredArgsConstructor
@Slf4j
public class TableController {
    @Autowired
    private TableService tableService;

    @PostMapping("/")
    @ResponseStatus(HttpStatus.CREATED)
    public void create(@RequestBody Table table) {
        tableService.createTable(table);
    }

    @GetMapping("/")
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<Table> get(@RequestParam("tableName") String tableName) {
        Table table = tableService.getTable(tableName);
        if (table == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(table);
    }
}