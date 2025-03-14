package com.bteshome.keyvaluestore.admindashboard.controller;

import com.bteshome.keyvaluestore.client.clientrequests.ItemDelete;
import com.bteshome.keyvaluestore.client.clientrequests.ItemList;
import com.bteshome.keyvaluestore.client.clientrequests.ItemQuery;
import com.bteshome.keyvaluestore.client.readers.ItemQuerier;
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

@Controller
@RequestMapping("/items/query")
@RequiredArgsConstructor
@Slf4j
public class ItemQueryController {
    @Autowired
    ItemQuerier itemQuerier;

    @GetMapping("/")
    public String query(Model model) {
        ItemQuery queryRequest = new ItemQuery();
        queryRequest.setTable("products");
        queryRequest.setLimit(10);
        queryRequest.setIndexName("category");
        queryRequest.setIndexKey("electronics");
        queryRequest.setIsolationLevel(IsolationLevel.READ_COMMITTED);
        model.addAttribute("queryRequest", queryRequest);
        model.addAttribute("page", "items-query");
        return "items-query.html";
    }

    @PostMapping("/")
    public String query(@ModelAttribute("queryRequest") @RequestBody ItemQuery queryRequest,
                        Model model) {
        try {
            ItemListResponseFlattened<String> response = itemQuerier
                    .queryForStrings(queryRequest)
                    .block();

            if (response != null && !response.getItems().isEmpty()) {
                model.addAttribute("response", response);
                model.addAttribute("hasMore", response.hasMore());
                model.addAttribute("cursorPositionsString", cursorPositionsToString(response));

                ItemDelete itemDeleteRequest = new ItemDelete();
                itemDeleteRequest.setTable(queryRequest.getTable());
                model.addAttribute("itemDeleteRequest", itemDeleteRequest);
            }
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
        }

        model.addAttribute("queryRequest", queryRequest);
        model.addAttribute("page", "items-query");
        return "items-query.html";
    }

    @PostMapping("/next/")
    public String next(@ModelAttribute("queryRequest") ItemQuery queryRequest,
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
                        queryRequest.getCursorPositions().put(partition, new CursorPosition(partition, itemKey, true));
                    }
                }
            }

            ItemListResponseFlattened<String> response = itemQuerier
                    .queryForStrings(queryRequest)
                    .block();

            if (response != null && !response.getItems().isEmpty()) {
                model.addAttribute("response", response);
                model.addAttribute("hasMore", response.hasMore());
                model.addAttribute("cursorPositionsString", cursorPositionsToString(response));

                ItemDelete itemDeleteRequest = new ItemDelete();
                itemDeleteRequest.setTable(queryRequest.getTable());
                model.addAttribute("itemDeleteRequest", itemDeleteRequest);
            }
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
        }

        model.addAttribute("queryRequest", queryRequest);
        model.addAttribute("page", "items-query");
        return "items-query.html";
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