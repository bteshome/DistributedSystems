package com.bteshome.keyvaluestore.admindashboard.controller;

import com.bteshome.keyvaluestore.client.clientrequests.*;
import com.bteshome.keyvaluestore.client.readers.ItemLister;
import com.bteshome.keyvaluestore.client.requests.IsolationLevel;
import com.bteshome.keyvaluestore.client.responses.CursorPosition;
import com.bteshome.keyvaluestore.client.responses.ItemListResponseFlattened;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.List;

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
            ItemListResponseFlattened<String> response = itemLister
                    .listStrings(listRequest)
                    .block();

            if (response != null && !response.getItems().isEmpty()) {
                model.addAttribute("response", response);
                model.addAttribute("hasMore", response.hasMore());
                model.addAttribute("cursorPositionsString", cursorPositionsToString(response));

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

    @PostMapping("/next/")
    public String next(@ModelAttribute("listRequest") ItemList listRequest,
                       @ModelAttribute("cursorPositionsString") String cursorPositionsString,
                       Model model) {
        try {
            if (Strings.isNotBlank(cursorPositionsString)) {
                String[] cursorPositions = cursorPositionsString.split(",");
                for (String cursorPosition : cursorPositions) {
                    String[] partitionKeyItemKey = cursorPosition.split(":");
                    if (partitionKeyItemKey.length == 2) {
                        int partition = Integer.parseInt(partitionKeyItemKey[0]);
                        String itemKey = partitionKeyItemKey[1];
                        listRequest.getCursorPositions().put(partition, new CursorPosition(partition, itemKey, true));
                    }
                }
            }

            ItemListResponseFlattened<String> response = itemLister
                    .listStrings(listRequest)
                    .block();

            if (response != null && !response.getItems().isEmpty()) {
                model.addAttribute("response", response);
                model.addAttribute("hasMore", response.hasMore());
                model.addAttribute("cursorPositionsString", cursorPositionsToString(response));

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

    private String cursorPositionsToString(ItemListResponseFlattened<String> response) {
        String cursorPositionsString = "";
        for (CursorPosition cursorPosition : response.getCursorPositions().values()) {
            if (cursorPosition != null) {
                int partition = cursorPosition.getPartition();
                String itemKey = cursorPosition.getLastReadItemKey();
                if (partition > 0 && Strings.isNotBlank(itemKey)) {
                    if (Strings.isNotBlank(cursorPositionsString))
                        cursorPositionsString += ",";
                    cursorPositionsString += (partition + ":" + itemKey);
                }
            }
        }
        return cursorPositionsString;
    }
}