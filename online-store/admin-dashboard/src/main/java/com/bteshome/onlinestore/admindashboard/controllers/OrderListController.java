package com.bteshome.onlinestore.admindashboard.controllers;

import com.bteshome.onlinestore.admindashboard.common.AppSettings;
import com.bteshome.onlinestore.admindashboard.dto.Order;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.List;

@Controller
@RequestMapping("/orders/list")
@RequiredArgsConstructor
@Slf4j
public class OrderListController {
    @Autowired
    WebClient webClient;
    @Autowired
    AppSettings appSettings;

    @GetMapping("/")
    public String list(Model model) {
        try {
            List<Order> orders = webClient
                    .get()
                    .uri("%s/api/orders/list/".formatted(appSettings.getOrderServiceUrl()))
                    .accept(MediaType.APPLICATION_JSON)
                    .retrieve()
                    .toEntityList(Order.class)
                    .block()
                    .getBody();

            if (orders != null && !orders.isEmpty())
                model.addAttribute("orders", orders);
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
        }

        model.addAttribute("page", "orders");
        return "orders-list.html";
    }
}
