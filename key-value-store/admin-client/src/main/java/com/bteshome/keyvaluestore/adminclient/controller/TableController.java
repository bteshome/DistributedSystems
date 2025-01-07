package com.bteshome.keyvaluestore.adminclient.controller;

import com.bteshome.keyvaluestore.adminclient.service.TableService;
import com.bteshome.keyvaluestore.common.requests.TableCreateRequest;
import com.bteshome.keyvaluestore.common.requests.TableGetRequest;
import com.bteshome.keyvaluestore.common.requests.TableListRequest;
import com.bteshome.keyvaluestore.common.responses.TableGetResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/tables")
@RequiredArgsConstructor
@Slf4j
public class TableController {
    @Autowired
    private TableService tableService;

    @PostMapping("/")
    public ResponseEntity<String> create(@RequestBody TableCreateRequest tableCreateResponse) {
        return tableService.createTable(tableCreateResponse);
    }

    @GetMapping("/")
    public ResponseEntity<?> list() {
        return tableService.list(new TableListRequest());
    }

    @GetMapping("/{tableName}/")
    public ResponseEntity<?> get(@PathVariable("tableName") String tableName) {
        return tableService.getTable(new TableGetRequest(tableName));
    }
}