package com.bteshome.keyvaluestore.admindashboard.controller;

import com.bteshome.keyvaluestore.client.clientrequests.*;
import com.bteshome.keyvaluestore.client.readers.ItemReader;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

@Controller
@RequestMapping("/items/get")
@RequiredArgsConstructor
@Slf4j
public class ItemGetController {
    @Autowired
    ItemReader itemReader;

    @GetMapping("/")
    public String get(Model model) {
        ItemGet request = new ItemGet();
        request.setTable("table1");
        request.setKey("key1");
        model.addAttribute("request", request);
        model.addAttribute("page", "items-get");
        return "items-get.html";
    }

    @PostMapping("/")
    public String get(@ModelAttribute("request") @RequestBody ItemGet request, Model model) {
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
}