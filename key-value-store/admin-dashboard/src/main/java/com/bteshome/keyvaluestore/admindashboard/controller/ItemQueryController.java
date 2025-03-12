package com.bteshome.keyvaluestore.admindashboard.controller;

import com.bteshome.keyvaluestore.client.clientrequests.ItemQuery;
import com.bteshome.keyvaluestore.client.readers.ItemQuerier;
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
    public String query(@ModelAttribute("queryRequest") @RequestBody ItemQuery queryRequest, Model model) {
        try {
            List<Tuple3<String, String, String>> response = itemQuerier
                    .queryForStrings(queryRequest)
                    .collectList()
                    .block();

            if (!response.isEmpty()) {
                model.addAttribute("items", response);
                model.addAttribute("itemsCount", response.size());
            }
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
        }

        model.addAttribute("queryRequest", queryRequest);
        model.addAttribute("page", "items-query");
        return "items-query.html";
    }
}