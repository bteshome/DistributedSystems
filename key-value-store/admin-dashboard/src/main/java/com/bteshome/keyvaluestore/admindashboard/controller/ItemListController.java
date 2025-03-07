package com.bteshome.keyvaluestore.admindashboard.controller;

import com.bteshome.keyvaluestore.client.clientrequests.*;
import com.bteshome.keyvaluestore.client.readers.ItemLister;
import com.bteshome.keyvaluestore.client.requests.IsolationLevel;
import com.bteshome.keyvaluestore.common.Tuple3;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/items/list")
@RequiredArgsConstructor
@Slf4j
public class ItemListController {
    @Autowired
    ItemLister itemLister;

    @GetMapping("/")
    public String list(Model model) {
        ItemList listRequest = new ItemList();
        listRequest.setTable("products");
        listRequest.setLimit(10);
        listRequest.setIsolationLevel(IsolationLevel.READ_COMMITTED);
        model.addAttribute("listRequest", listRequest);
        model.addAttribute("page", "items-list");
        return "items-list.html";
    }

    @PostMapping("/")
    public String list(@ModelAttribute("listRequest") @RequestBody ItemList listRequest, Model model) {
        try {
            List<Tuple3<String, String, String>> response = itemLister
                    .listStrings(listRequest)
                    .collectList()
                    .block();

            if (!response.isEmpty()) {
                model.addAttribute("items", response);
                model.addAttribute("itemsCount", response.size());

                // TODO - first make the api return the partition key with the item
                BatchDelete batchDeleteRequest = new BatchDelete();
                batchDeleteRequest.setTable("table1");
                batchDeleteRequest.getKeys().addAll(response.stream().map(Tuple3::first).toList());
                batchDeleteRequest.getPartitionKeys().addAll(response.stream().map(Tuple3::second).toList());
                model.addAttribute("batchDeleteRequest", batchDeleteRequest);

                ItemDelete itemDeleteRequest = new ItemDelete();
                itemDeleteRequest.setTable(listRequest.getTable());
                model.addAttribute("itemDeleteRequest", itemDeleteRequest);
            }
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
        }

        model.addAttribute("listRequest", listRequest);
        model.addAttribute("page", "items-list");
        return "items-list.html";
    }
}