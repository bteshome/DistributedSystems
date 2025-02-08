package com.bteshome.keyvaluestore.admindashboard.controller;

import com.bteshome.keyvaluestore.admindashboard.common.AdminDashboardException;
import com.bteshome.keyvaluestore.admindashboard.dto.ItemPutBatchDto;
import com.bteshome.keyvaluestore.client.clientrequests.*;
import com.bteshome.keyvaluestore.client.requests.AckType;
import com.bteshome.keyvaluestore.client.responses.ItemPutResponse;
import com.bteshome.keyvaluestore.client.writers.BatchWriter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.Map;
import java.util.Random;

@Controller
@RequestMapping("/items/put-batch")
@RequiredArgsConstructor
@Slf4j
public class ItemPutBatchController {
    @Autowired
    BatchWriter batchWriter;

    @GetMapping("/")
    public String putBatch(Model model) {
        ItemPutBatchDto request = new ItemPutBatchDto();
        request.setTable("table1");
        request.setNumItems(10);
        request.setAck(AckType.MIN_ISR_COUNT);
        request.setMaxRetries(0);
        model.addAttribute("request", request);
        model.addAttribute("page", "items-put-batch");
        return "items-put-batch.html";
    }

    @PostMapping("/")
    public String putBatch(@ModelAttribute("request") @RequestBody ItemPutBatchDto request, Model model) {
        try {
            Random random = new Random();
            BatchWrite<String> batchWrite = new BatchWrite<>();
            batchWrite.setTable(request.getTable());
            batchWrite.setAck(request.getAck());
            batchWrite.setMaxRetries(request.getMaxRetries());
            Instant now = Instant.now();
            for (int i = 0; i < request.getNumItems(); i++) {
                String key = "key" + now.toEpochMilli() + "_" + i;
                String value = "value" + now.toEpochMilli() + "_" + i;
                batchWrite.getItems().add(Map.entry(key, value));
            }

            ItemPutResponse lastPartitionResponse = batchWriter.putStringBatch(batchWrite).blockLast();
            if (lastPartitionResponse.getHttpStatusCode() != 200) {
                throw new AdminDashboardException("PUT failed. Http status: %s, error: %s, end offset: %s.".formatted(
                        lastPartitionResponse.getHttpStatusCode(),
                        lastPartitionResponse.getErrorMessage(),
                        lastPartitionResponse.getEndOffset()));
            }

            model.addAttribute("info","PUT succeeded.");
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
        }

        model.addAttribute("request", request);
        model.addAttribute("page", "items-put-batch");
        return "items-put-batch.html";
    }
}