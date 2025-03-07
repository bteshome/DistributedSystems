package com.bteshome.keyvaluestore.admindashboard.controller;

import com.bteshome.keyvaluestore.admindashboard.common.AdminDashboardException;
import com.bteshome.keyvaluestore.admindashboard.dto.ItemPutDto;
import com.bteshome.keyvaluestore.admindashboard.model.Product;
import com.bteshome.keyvaluestore.client.clientrequests.*;
import com.bteshome.keyvaluestore.client.requests.AckType;
import com.bteshome.keyvaluestore.client.responses.ItemPutResponse;
import com.bteshome.keyvaluestore.client.writers.ItemWriter;
import com.bteshome.keyvaluestore.common.JsonSerDe;
import com.bteshome.keyvaluestore.common.MetadataCache;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@Controller
@RequestMapping("/items/put")
@RequiredArgsConstructor
@Slf4j
public class ItemPutController {
    @Autowired
    private ItemWriter itemWriter;

    @GetMapping("/")
    public String put(Model model) {
        ItemPutDto itemPutDto = new ItemPutDto();
        itemPutDto.setTable("products");
        itemPutDto.setKey("p1");
        itemPutDto.setAck(AckType.MIN_ISR_COUNT);
        itemPutDto.setMaxRetries(0);
        String value = """
                {
                  "category": "electronics",
                  "subcategory": "phones",
                  "name": "iPhone 16",
                  "price": 899.99
                }""";
        itemPutDto.setValue(value);
        itemPutDto.setIndexKeys("category=electronics,subcategory=phones");
        model.addAttribute("itemPutDto", itemPutDto);
        model.addAttribute("page", "items-put");
        return "items-put.html";
    }

    @PostMapping("/")
    public String put(@ModelAttribute("itemPutDto") @RequestBody ItemPutDto itemPutDto, Model model) {
        try {
            ItemWrite<Product> request = new ItemWrite<>();
            request.setTable(itemPutDto.getTable());
            request.setKey(itemPutDto.getKey());
            request.setPartitionKey(itemPutDto.getPartitionKey());
            request.setAck(itemPutDto.getAck());
            request.setMaxRetries(itemPutDto.getMaxRetries());
            Product value = JsonSerDe.deserialize(itemPutDto.getValue(), Product.class);
            request.setValue(value);
            if (Strings.isNotBlank(itemPutDto.getIndexKeys())) {
                request.setIndexKeys(new HashMap<>());
                for (String indexNameAndKey : itemPutDto.getIndexKeys().split(",")) {
                    String[] parts = indexNameAndKey.split("=");
                    String indexName = parts[0];
                    String indexKey = parts[1];
                    request.getIndexKeys().put(indexName, indexKey);
                }
            }
            ItemPutResponse response = itemWriter.putObject(request).block();
            if (response.getHttpStatusCode() != 200) {
                throw new AdminDashboardException("PUT failed. Http status: %s, error: %s, end offset: %s.".formatted(
                        response.getHttpStatusCode(),
                        response.getErrorMessage(),
                        response.getEndOffset()));
            }
            itemPutDto.setKey("");
            model.addAttribute("info", "PUT succeeded.");
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
        }

        model.addAttribute("itemPutDto", itemPutDto);
        model.addAttribute("page", "items-put");
        return "items-put.html";
    }
}