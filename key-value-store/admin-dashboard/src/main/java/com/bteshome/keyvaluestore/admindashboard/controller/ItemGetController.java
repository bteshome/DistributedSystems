package com.bteshome.keyvaluestore.admindashboard.controller;

import com.bteshome.keyvaluestore.client.adminreaders.ItemVersionsReader;
import com.bteshome.keyvaluestore.client.clientrequests.*;
import com.bteshome.keyvaluestore.client.readers.ItemReader;
import com.bteshome.keyvaluestore.common.LogPosition;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/items/get")
@RequiredArgsConstructor
@Slf4j
public class ItemGetController {
    @Autowired
    ItemReader itemReader;
    @Autowired
    ItemVersionsReader itemVersionsReader;

    @GetMapping("/")
    public String get(Model model) {
        ItemGet request = new ItemGet();
        request.setTable("products");
        request.setKey("p1");
        request.setKey("p1");
        model.addAttribute("request", request);
        model.addAttribute("page", "items-get");
        return "items-get.html";
    }

    @PostMapping("/")
    public String getCommittedVersion(@ModelAttribute("request") @RequestBody ItemGet request, Model model) {
        try {
            String response = itemReader.getString(request).block();
            model.addAttribute("item", response);
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
        }

        model.addAttribute("request", request);
        model.addAttribute("page", "items-get");
        return "items-get.html";
    }

    @PostMapping("/versions/")
    public String getAllVersions(@ModelAttribute("request") @RequestBody ItemGet request, Model model) {
        try {
            List<String> response = itemVersionsReader.getString(request).block();
            model.addAttribute("itemVersions", response);
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
        }

        model.addAttribute("request", request);
        model.addAttribute("page", "items-get");
        return "items-get.html";
    }
}