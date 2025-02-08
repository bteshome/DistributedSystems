package com.bteshome.keyvaluestore.admindashboard.controller;

import com.bteshome.keyvaluestore.admindashboard.common.AdminDashboardException;
import com.bteshome.keyvaluestore.client.clientrequests.*;
import com.bteshome.keyvaluestore.client.deleters.ItemDeleter;
import com.bteshome.keyvaluestore.client.requests.AckType;
import com.bteshome.keyvaluestore.client.responses.ItemDeleteResponse;
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
public class ItemDeleteController {
    @Autowired
    ItemDeleter itemDeleter;

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
            ItemDeleteResponse response = itemDeleter.delete(itemDeleteRequest).block();
            if (response.getHttpStatusCode() != 200) {
                throw new AdminDashboardException("DELETE failed. Http status: %s, error: %s, end offset: %s.".formatted(
                        response.getHttpStatusCode(),
                        response.getErrorMessage(),
                        response.getEndOffset()));
            }
            model.addAttribute("info", "DELETE succeeded.");
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
            return "items-list.html";
        }

        ItemList listRequest = new ItemList();
        listRequest.setTable(itemDeleteRequest.getTable());
        listRequest.setLimit(10);
        model.addAttribute("listRequest", listRequest);
        model.addAttribute("page", "items-list");
        return "items-list.html";
    }
}