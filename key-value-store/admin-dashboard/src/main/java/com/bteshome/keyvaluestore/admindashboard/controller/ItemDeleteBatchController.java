package com.bteshome.keyvaluestore.admindashboard.controller;

import com.bteshome.keyvaluestore.admindashboard.common.AdminDashboardException;
import com.bteshome.keyvaluestore.client.clientrequests.*;
import com.bteshome.keyvaluestore.client.deleters.BatchDeleter;
import com.bteshome.keyvaluestore.client.requests.AckType;
import com.bteshome.keyvaluestore.client.responses.ItemDeleteResponse;
import com.bteshome.keyvaluestore.client.responses.ItemPutResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

@Controller
@RequestMapping("/items")
@RequiredArgsConstructor
@Slf4j
public class ItemDeleteBatchController {
    @Autowired
    BatchDeleter batchDeleter;

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
                batchDeleter.deleteBatch(batchDeleteRequest);

                ItemDeleteResponse lastPartitionResponse = batchDeleter.deleteBatch(batchDeleteRequest).blockLast();
                if (lastPartitionResponse.getHttpStatusCode() != 200) {
                    throw new AdminDashboardException("DELETE failed. Http status: %s, error: %s, end offset: %s.".formatted(
                            lastPartitionResponse.getHttpStatusCode(),
                            lastPartitionResponse.getErrorMessage(),
                            lastPartitionResponse.getEndOffset()));
                }

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