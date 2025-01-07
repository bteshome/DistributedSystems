package com.bteshome.keyvaluestore.adminclient.controller;

import com.bteshome.keyvaluestore.adminclient.service.NodeService;
import com.bteshome.keyvaluestore.common.requests.StorageNodeGetRequest;
import com.bteshome.keyvaluestore.common.requests.StorageNodeListRequest;
import com.bteshome.keyvaluestore.common.responses.StorageNodeGetResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/nodes")
@RequiredArgsConstructor
@Slf4j
public class NodeController {
    @Autowired
    private NodeService nodeService;

    @GetMapping("/{id}/")
    public ResponseEntity<?> get(@PathVariable("id") String id) {
        return nodeService.getNode(new StorageNodeGetRequest(id));
    }

    @GetMapping("/")
    public ResponseEntity<?> list() {
        return nodeService.list(new StorageNodeListRequest());
    }
}