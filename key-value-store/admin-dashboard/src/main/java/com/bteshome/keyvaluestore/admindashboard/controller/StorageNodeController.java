package com.bteshome.keyvaluestore.admindashboard.controller;

import com.bteshome.keyvaluestore.admindashboard.service.NodeService;
import com.bteshome.keyvaluestore.common.requests.StorageNodeListRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("/nodes/storage")
@RequiredArgsConstructor
@Slf4j
public class StorageNodeController {
    @Autowired
    private NodeService nodeService;

    @GetMapping("/")
    public String list(Model model) {
        var nodes = nodeService.list(new StorageNodeListRequest());
        model.addAttribute("nodes", nodes);
        model.addAttribute("page", "storage_nodes");
        return "storage-nodes.html";
    }
}