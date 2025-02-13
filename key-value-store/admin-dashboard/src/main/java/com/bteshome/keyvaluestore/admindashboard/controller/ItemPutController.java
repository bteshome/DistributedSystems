package com.bteshome.keyvaluestore.admindashboard.controller;

import com.bteshome.keyvaluestore.admindashboard.common.AdminDashboardException;
import com.bteshome.keyvaluestore.client.clientrequests.*;
import com.bteshome.keyvaluestore.client.requests.AckType;
import com.bteshome.keyvaluestore.client.responses.ItemPutResponse;
import com.bteshome.keyvaluestore.client.writers.ItemWriter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

@Controller
@RequestMapping("/items/put")
@RequiredArgsConstructor
@Slf4j
public class ItemPutController {
    @Autowired
    private ItemWriter itemWriter;

    @GetMapping("/")
    public String put(Model model) {
        ItemWrite<String> request = new ItemWrite<>();
        request.setTable("table1");
        request.setKey("key1");
        request.setAck(AckType.MIN_ISR_COUNT);
        request.setMaxRetries(0);
        String value = """
                {
                  "id": 1,
                  "name": "Wireless Headphones",
                  "price": 199.99
                }""";
        request.setValue(value);
        model.addAttribute("request", request);
        model.addAttribute("page", "items-put");
        return "items-put.html";
    }

    @PostMapping("/")
    public String put(@ModelAttribute("request") @RequestBody ItemWrite<String> request, Model model) {
        try {
            ItemPutResponse response = itemWriter.putString(request).block();
            if (response.getHttpStatusCode() != 200) {
                throw new AdminDashboardException("PUT failed. Http status: %s, error: %s, end offset: %s.".formatted(
                        response.getHttpStatusCode(),
                        response.getErrorMessage(),
                        response.getEndOffset()));
            }
            request.setKey("");
            model.addAttribute("info", "PUT succeeded.");
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
        }

        model.addAttribute("request", request);
        model.addAttribute("page", "items-put");
        return "items-put.html";
    }
}