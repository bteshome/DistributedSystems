package com.bteshome.keyvaluestore.admindashboard.controller;

import com.bteshome.keyvaluestore.admindashboard.dto.ItemPutBatchDto;
import com.bteshome.keyvaluestore.client.*;
import com.bteshome.keyvaluestore.client.clientrequests.BatchWrite;
import com.bteshome.keyvaluestore.client.clientrequests.ItemGet;
import com.bteshome.keyvaluestore.client.clientrequests.ItemList;
import com.bteshome.keyvaluestore.client.clientrequests.ItemWrite;
import com.bteshome.keyvaluestore.common.entities.Item;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

@Controller
@RequestMapping("/items")
@RequiredArgsConstructor
@Slf4j
public class ItemController {
    @Autowired
    ItemWriter itemWriter;

    @Autowired
    ItemReader itemReader;

    @Autowired
    BatchWriter batchWriter;

    @Autowired
    BatchReader batchReader;

    @GetMapping("/get/")
    public String get(Model model) {
        ItemGet request = new ItemGet();
        request.setTable("table1");
        request.setKey("key1");
        model.addAttribute("request", request);
        model.addAttribute("page", "items-get");
        return "items-get.html";
    }

    @PostMapping("/get/")
    public String get(@ModelAttribute("request") @RequestBody ItemGet request, Model model) {
        try {
            String response = itemReader.getString(request);
            model.addAttribute("item", response);
            model.addAttribute("request", request);
            model.addAttribute("page", "items-get");
            return "items-get.html";
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
            model.addAttribute("request", request);
            model.addAttribute("page", "items-get");
            return "items-get.html";
        }
    }

    @GetMapping("/list/")
    public String list(Model model) {
        ItemList request = new ItemList();
        request.setTable("table1");
        request.setLimit(10);
        model.addAttribute("request", request);
        model.addAttribute("page", "items-list");
        return "items-list.html";
    }

    @PostMapping("/list/")
    public String list(@ModelAttribute("request") @RequestBody ItemList request, Model model) {
        try {
            Set<Map.Entry<Integer, List<Map.Entry<String, String>>>> response = batchReader.listStrings(request).entrySet();
            if (!response.isEmpty())
                model.addAttribute("items", response);
            model.addAttribute("request", request);
            model.addAttribute("page", "items-list");
            return "items-list.html";
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
            model.addAttribute("request", request);
            model.addAttribute("page", "items-list");
            return "items-list.html";
        }
    }

    @GetMapping("/put/")
    public String put(Model model) {
        ItemWrite<String> request = new ItemWrite<>();
        request.setTable("table1");
        request.setKey("key1");
        String value = """
                {
                  "id": 1,
                  "name": "Wireless Headphones",
                  "description": "High-quality noise-canceling headphones with Bluetooth connectivity.",
                  "price": 199.99,
                  "currency": "USD",
                  "available": true
                }""";
        request.setValue(value);
        model.addAttribute("request", request);
        model.addAttribute("page", "items-put");
        return "items-put.html";
    }

    @PostMapping("/put/")
    public String put(@ModelAttribute("request") @RequestBody ItemWrite<String> request, Model model) {
        try {
            itemWriter.putString(request);
            return "redirect:/items/get/";
        } catch (Exception e) {
            model.addAttribute("request", request);
            model.addAttribute("page", "items-put");
            model.addAttribute("error", e.getMessage());
            return "items-put.html";
        }
    }

    @GetMapping("/put-bulk/")
    public String putBulk(Model model) {
        ItemPutBatchDto request = new ItemPutBatchDto();
        request.setTable("table1");
        request.setNumItems(10);
        model.addAttribute("request", request);
        model.addAttribute("page", "items-put-bulk");
        return "items-put-bulk.html";
    }

    @PostMapping("/put-bulk/")
    public String putBulk(@ModelAttribute("request") @RequestBody ItemPutBatchDto request, Model model) {
        try {
            Random random = new Random();
            BatchWrite<String> batchWrite = new BatchWrite<>();
            batchWrite.setTable(request.getTable());
            for (int i = 0; i < request.getNumItems(); i++) {
                int randomNumber = random.nextInt(1, Integer.MAX_VALUE);
                String key = "key" + randomNumber;
                String value = "value" + randomNumber;
                batchWrite.getItems().add(Map.entry(key, value));
            }
            batchWriter.putStringBatch(batchWrite);
            return "redirect:/items/get/";
        } catch (Exception e) {
            model.addAttribute("request", request);
            model.addAttribute("page", "items-put-bulk");
            model.addAttribute("error", e.getMessage());
            return "items-put-bulk.html";
        }
    }
}