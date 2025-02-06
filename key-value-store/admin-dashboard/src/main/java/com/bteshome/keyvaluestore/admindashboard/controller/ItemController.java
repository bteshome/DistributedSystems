package com.bteshome.keyvaluestore.admindashboard.controller;

import com.bteshome.keyvaluestore.admindashboard.dto.ItemDeleteBatchDto;
import com.bteshome.keyvaluestore.admindashboard.dto.ItemPutBatchDto;
import com.bteshome.keyvaluestore.client.*;
import com.bteshome.keyvaluestore.client.clientrequests.*;
import com.bteshome.keyvaluestore.client.requests.AckType;
import com.bteshome.keyvaluestore.client.requests.ItemDeleteRequest;
import com.bteshome.keyvaluestore.common.entities.Item;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
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
        ItemList listRequest = new ItemList();
        listRequest.setTable("table1");
        listRequest.setLimit(10);
        model.addAttribute("listRequest", listRequest);
        model.addAttribute("page", "items-list");
        return "items-list.html";
    }

    @PostMapping("/list/")
    public String list(@ModelAttribute("listRequest") @RequestBody ItemList listRequest, Model model) {
        try {
            Map<Integer, List<Map.Entry<String, String>>> response = batchReader.listStrings(listRequest);
            if (!response.isEmpty()) {
                model.addAttribute("items", response.entrySet());
                long count = response.values().stream().mapToLong(List::size).sum();
                model.addAttribute("itemsCount", count);

                BatchDelete batchDeleteRequest = new BatchDelete();
                batchDeleteRequest.setTable("table1");
                batchDeleteRequest.setKeys(response.values().stream().flatMap(List::stream).map(Map.Entry::getKey).toList());
                model.addAttribute("batchDeleteRequest", batchDeleteRequest);

                ItemDelete itemDeleteRequest = new ItemDelete();
                itemDeleteRequest.setTable(listRequest.getTable());
                model.addAttribute("itemDeleteRequest", itemDeleteRequest);
            }

            model.addAttribute("listRequest", listRequest);
            model.addAttribute("page", "items-list");
            return "items-list.html";
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
            model.addAttribute("listRequest", listRequest);
            model.addAttribute("page", "items-list");
            return "items-list.html";
        }
    }

    @GetMapping("/put/")
    public String put(Model model) {
        ItemWrite<String> request = new ItemWrite<>();
        request.setTable("table1");
        request.setKey("key1");
        request.setAck(AckType.MIN_ISR_COUNT);
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
        request.setAck(AckType.MIN_ISR_COUNT);
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
            batchWrite.setAck(request.getAck());
            Instant now = Instant.now();
            for (int i = 0; i < request.getNumItems(); i++) {
                String key = "key" + now.toEpochMilli() + "_" + i;
                String value = "value" + now.toEpochMilli() + "_" + i;
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

    @PostMapping("/delete-item/")
    public String deleteItem(@ModelAttribute("itemDeleteRequest") @RequestBody ItemDelete itemDeleteRequest, Model model) {
        try {
            model.addAttribute("promptItemDelete", "Are you sure you want to delete the item with key '%s' in table '%s'?".formatted(
                    itemDeleteRequest.getKey(),
                    itemDeleteRequest.getTable()));
            model.addAttribute("itemDeleteRequest", itemDeleteRequest);
            model.addAttribute("page", "items-list");
            return "items-list.html";
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
            model.addAttribute("page", "items-list");
            return "items-list.html";
        }
    }

    @PostMapping("/delete-item-confirmed/")
    public String deleteItemConfirmed(@ModelAttribute("itemDeleteRequest") @RequestBody ItemDelete itemDeleteRequest, Model model) {
        try {
            itemDeleteRequest.setAck(AckType.MIN_ISR_COUNT);
            itemWriter.delete(itemDeleteRequest);
            model.addAttribute("info", "Deleted item with key '%s' in table '%s'.".formatted(
                    itemDeleteRequest.getKey(),
                    itemDeleteRequest.getTable()));
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
        }

        ItemList listRequest = new ItemList();
        listRequest.setTable(itemDeleteRequest.getTable());
        listRequest.setLimit(10);
        model.addAttribute("listRequest", listRequest);
        model.addAttribute("page", "items-list");
        return "items-list.html";
    }

    @PostMapping("/delete-batch/")
    public String deleteBatch(@ModelAttribute("batchDeleteRequest") @RequestBody BatchDelete batchDeleteRequest, Model model) {
        try {
            if (!batchDeleteRequest.getKeys().isEmpty()) {
                model.addAttribute("promptBatchDelete",
                        "Are you sure you want to delete these %s items?".formatted(batchDeleteRequest.getKeys().size()));
                model.addAttribute("batchDeleteRequest", batchDeleteRequest);
            } else {
                ItemList listRequest = new ItemList();
                listRequest.setTable(batchDeleteRequest.getTable());
                listRequest.setLimit(10);
                model.addAttribute("listRequest", listRequest);
            }
            model.addAttribute("page", "items-list");
            return "items-list.html";
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
            model.addAttribute("page", "items-list");
            return "items-list.html";
        }
    }

    @PostMapping("/delete-batch-confirmed/")
    public String deleteBatchConfirmed(@ModelAttribute("batchDeleteRequest") @RequestBody BatchDelete batchDeleteRequest, Model model) {
        try {
            if (!batchDeleteRequest.getKeys().isEmpty()) {
                batchDeleteRequest.setAck(AckType.MIN_ISR_COUNT);
                batchWriter.deleteBatch(batchDeleteRequest);
                model.addAttribute("info", "Deleted %s items.".formatted(batchDeleteRequest.getKeys().size()));
            }
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
        }

        ItemList listRequest = new ItemList();
        listRequest.setTable(batchDeleteRequest.getTable());
        listRequest.setLimit(10);
        model.addAttribute("listRequest", listRequest);
        model.addAttribute("page", "items-list");
        return "items-list.html";
    }
}