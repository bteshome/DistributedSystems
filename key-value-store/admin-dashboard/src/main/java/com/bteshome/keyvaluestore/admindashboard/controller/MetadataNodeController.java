package com.bteshome.keyvaluestore.admindashboard.controller;

import com.bteshome.keyvaluestore.admindashboard.service.NodeService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

@Controller
@RequestMapping("/")
@RequiredArgsConstructor
@Slf4j
public class MetadataNodeController {
    @Autowired
    private NodeService nodeService;

    @GetMapping
    public String list(Model model) {
        model.addAttribute("nodes", new ArrayList<String>());
        model.addAttribute("page", "metadata_nodes");
        return "metadata-nodes";
    }
}
