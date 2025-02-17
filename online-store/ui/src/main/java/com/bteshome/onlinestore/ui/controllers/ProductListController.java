package com.bteshome.onlinestore.ui.controllers;

import com.bteshome.onlinestore.ui.common.AppSettings;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
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
@RequestMapping("/")
@RequiredArgsConstructor
@Slf4j
public class ProductListController {
    @Autowired
    WebClient webClient;
    @Autowired
    AppSettings appSettings;

    @GetMapping
    public String list(Model model, HttpServletRequest httpServletRequest) {
        try {
            List<?> products = webClient
                    .get()
                    .uri("%s/api/products/".formatted(appSettings.getInventoryServiceUrl()))
                    .accept(MediaType.APPLICATION_JSON)
                    .retrieve()
                    .toEntity(List.class)
                    .block()
                    .getBody();

            List<String> cart = getCartFromCookie(httpServletRequest);

            model.addAttribute("cartSize", cart.size());
            model.addAttribute("products", products);
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
        }

        model.addAttribute("page", "products");
        return "products-list.html";
    }

    private List<String> getCartFromCookie(HttpServletRequest httpServletRequest) throws IOException {
        Cookie cartCookie = WebUtils.getCookie(httpServletRequest, "cart");
        List<String> cart = new ArrayList<>();

        if (cartCookie != null) {
            String cartItems = cartCookie.getValue();
            cart.addAll(Arrays.asList(cartItems.split("\\|")));
        }

        return cart;
    }
}