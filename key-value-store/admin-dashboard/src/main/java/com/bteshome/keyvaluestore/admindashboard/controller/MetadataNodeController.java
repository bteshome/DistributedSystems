package com.bteshome.keyvaluestore.admindashboard.controller;

import com.bteshome.keyvaluestore.admindashboard.service.MetadataNodeService;
import com.bteshome.keyvaluestore.common.MetadataClientSettings;
import com.bteshome.keyvaluestore.common.entities.RaftPeerInfo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Controller
@RequestMapping("/")
@RequiredArgsConstructor
@Slf4j
public class MetadataNodeController {
    @Autowired
    private MetadataNodeService nodeService;

    @GetMapping
    public String list(Model model) {
        try {
            List<RaftPeerInfo> nodes = nodeService.list();
            model.addAttribute("nodes", nodes);
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
        }
        model.addAttribute("page", "metadata_nodes");
        return "metadata-nodes";
    }
}
