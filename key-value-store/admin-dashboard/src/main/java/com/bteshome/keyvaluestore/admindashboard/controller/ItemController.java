package com.bteshome.keyvaluestore.admindashboard.controller;

import com.bteshome.keyvaluestore.admindashboard.dto.ItemGetOrListRequest;
import com.bteshome.keyvaluestore.admindashboard.dto.ItemPutBatchRequest;
import com.bteshome.keyvaluestore.client.*;
import com.bteshome.keyvaluestore.client.requests.ItemGetRequest;
import com.bteshome.keyvaluestore.client.requests.ItemListRequest;
import com.bteshome.keyvaluestore.client.requests.ItemPutRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.thirdparty.com.google.common.base.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.Random;

@Controller
@RequestMapping("/items")
@RequiredArgsConstructor
@Slf4j
public class ItemController {
    @Autowired
    ItemWriter itemWriter;

    @Autowired
    ItemReader itemReader;

    @GetMapping("/")
    public String getOrList(Model model) {
        ItemGetOrListRequest request = new ItemGetOrListRequest();
        request.setTable("table1");
        request.setPartition(1);
        request.setLimit(10);
        model.addAttribute("request", request);
        model.addAttribute("page", "items");
        return "items-get-or-list.html";
    }

    @PostMapping("/")
    public String getOrList(@ModelAttribute("request") @RequestBody ItemGetOrListRequest request, Model model) {
        try {
            if (Strings.isNullOrEmpty(request.getKey())) {
                ItemListRequest listRequest = new ItemListRequest();
                listRequest.setTable(request.getTable());
                listRequest.setPartition(request.getPartition());
                listRequest.setLimit(request.getLimit());
                List<Map.Entry<String, String>> response = itemReader.list(listRequest);
                if (!response.isEmpty()) {
                    model.addAttribute("items", response);
                }
            } else {
                ItemGetRequest getRequest = new ItemGetRequest();
                getRequest.setTable(request.getTable());
                getRequest.setKey(request.getKey());
                String response = itemReader.get(getRequest);
                model.addAttribute("item", response);
            }
            model.addAttribute("request", request);
            model.addAttribute("page", "items");
            return "items-get-or-list.html";
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
            model.addAttribute("request", request);
            model.addAttribute("page", "items");
            return "items-get-or-list.html";
        }
    }

    @GetMapping("/put/")
    public String put(Model model) {
        ItemPutRequest request = new ItemPutRequest();
        request.setTable("table1");
        request.setKey("key1");
        request.setValue("value1");
        model.addAttribute("request", request);
        model.addAttribute("page", "items-put");
        return "items-put.html";
    }

    @PostMapping("/put/")
    public String put(@ModelAttribute("item") @RequestBody ItemPutRequest request, Model model) {
        try {
            itemWriter.put(request);
            return "redirect:/items/";
        } catch (Exception e) {
            model.addAttribute("request", request);
            model.addAttribute("page", "items-put");
            model.addAttribute("error", e.getMessage());
            return "items-put.html";
        }
    }

    @GetMapping("/put-batch/")
    public String putBulk(Model model) {
        ItemPutBatchRequest request = new ItemPutBatchRequest();
        request.setTable("table1");
        request.setNumItems(10);
        model.addAttribute("request", request);
        model.addAttribute("page", "items-put-batch");
        return "items-put-batch.html";
    }

    @PostMapping("/put-batch/")
    public String putBulk(@ModelAttribute("item") @RequestBody ItemPutBatchRequest request, Model model) {
        try {
            Random random = new Random();
            for (int i = 0; i < request.getNumItems(); i++) {
                ItemPutRequest itemPutRequest = new ItemPutRequest();
                itemPutRequest.setTable(request.getTable());
                int randonNumber = random.nextInt(1, Integer.MAX_VALUE);
                itemPutRequest.setKey("first" + randonNumber);
                itemPutRequest.setValue("second" + randonNumber);
                itemWriter.put(itemPutRequest);
            }
            return "redirect:/items/";
        } catch (Exception e) {
            model.addAttribute("request", request);
            model.addAttribute("page", "items-put-batch");
            model.addAttribute("error", e.getMessage());
            return "items-put-batch.html";
        }
    }
}