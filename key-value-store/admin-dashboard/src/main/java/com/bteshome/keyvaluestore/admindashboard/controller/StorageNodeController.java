package com.bteshome.keyvaluestore.admindashboard.controller;

import com.bteshome.keyvaluestore.admindashboard.service.NodeService;
import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.common.requests.StorageNodeLeaveRequest;
import com.bteshome.keyvaluestore.common.requests.StorageNodeListRequest;
import com.bteshome.keyvaluestore.common.requests.TableCreateRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

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

    @GetMapping("/remove/")
    public String create(@RequestParam String id, Model model) {
        StorageNodeLeaveRequest request = new StorageNodeLeaveRequest();
        request.setId(id);
        model.addAttribute("request", request);
        model.addAttribute("page", "storage_nodes");
        return "storage-nodes-remove.html";
    }

    @PostMapping("/remove/")
    public String create(@ModelAttribute("request") @RequestBody StorageNodeLeaveRequest request, Model model) {
        try {
            request.validate();
            nodeService.removeNode(request);
            // TODO - remove
            model.addAttribute("request", request);
            model.addAttribute("page", "storage_nodes");
            return "storage-nodes-remove-confirmation.html";
        } catch (Exception e) {
            model.addAttribute("request", request);
            model.addAttribute("error", e.getMessage());
            model.addAttribute("page", "storage_nodes");
            return "storage-nodes-remove.html";
        }
    }
}
