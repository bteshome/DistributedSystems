package com.bteshome.onlinestore.ui.controllers;

import com.bteshome.onlinestore.ui.common.AppSettings;
import com.bteshome.onlinestore.ui.dto.LineItem;
import com.bteshome.onlinestore.ui.dto.Order;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.WebUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Controller
@RequestMapping("/orders/create")
@RequiredArgsConstructor
@Slf4j
public class OrderCreateController {
    @Autowired
    WebClient webClient;
    @Autowired
    AppSettings appSettings;

    @GetMapping("/")
    public String create(Model model, HttpServletRequest httpServletRequest) {
        try {
            List<LineItem> lineItems = getLineItemsFromCookie(httpServletRequest);

            if (lineItems.isEmpty())
                return "redirect:/";

            model.addAttribute("lineItems", lineItems);
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
        }

        model.addAttribute("page", "orders");
        return "orders-create.html";
    }

    @PostMapping("/")
    public String create(@RequestParam String email,
                         Model model,
                         HttpServletRequest httpServletRequest,
                         HttpServletResponse httpServletResponse) {
        try {
            List<LineItem> lineItems = getLineItemsFromCookie(httpServletRequest);

            if (lineItems.isEmpty()) {
                model.addAttribute("error","The cart is empty.");
            } else {
                Order order = new Order();
                order.setLineItems(lineItems);
                order.setEmail(email);

                String response = webClient
                        .post()
                        .uri("%s/api/orders/".formatted(appSettings.getOrderServiceUrl()))
                        .contentType(MediaType.APPLICATION_JSON)
                        .accept(MediaType.APPLICATION_JSON)
                        .bodyValue(order)
                        .retrieve()
                        .toEntity(String.class)
                        .block()
                        .getBody();

                clearCart(httpServletResponse);
                model.addAttribute("info", response);
            }
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
        }

        model.addAttribute("page", "orders");
        return "orders-create.html";
    }

    private List<LineItem> getLineItemsFromCookie(HttpServletRequest httpServletRequest) throws IOException {
        Cookie cartCookie = WebUtils.getCookie(httpServletRequest, "cart");
        List<LineItem> lineItems = new ArrayList<>();

        if (cartCookie != null) {
            String[] cartItems = cartCookie.getValue().split("\\|");
            lineItems.addAll(Arrays.stream(cartItems)
                    .map(LineItem::fromString)
                    .toList());
        }

        return lineItems;
    }

    private void clearCart(HttpServletResponse response) {
        Cookie cartCookie = new Cookie("cart", "");
        cartCookie.setMaxAge(0);
        cartCookie.setPath("/");
        response.addCookie(cartCookie);
    }
}