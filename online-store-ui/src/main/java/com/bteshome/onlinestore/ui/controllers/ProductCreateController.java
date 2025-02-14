package com.bteshome.onlinestore.ui.controllers;

import com.bteshome.onlinestore.ui.common.AppSettings;
import com.bteshome.onlinestore.ui.common.Validator;
import com.bteshome.onlinestore.ui.dto.Product;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.reactive.function.client.WebClient;

import java.math.BigDecimal;

@Controller
@RequestMapping("/products/create")
@RequiredArgsConstructor
@Slf4j
public class ProductCreateController {
    @Autowired
    WebClient webClient;
    @Autowired
    AppSettings appSettings;

    @GetMapping("/")
    public String create(Model model) {
        Product product = new Product();
        product.setName("product1");
        product.setDescription("an iphone");
        product.setPrice(BigDecimal.valueOf(23.1D));
        product.setSkuCode("product1");
        product.setStockLevel(10);
        model.addAttribute("product", product);
        model.addAttribute("page", "products");
        return "products-create.html";
    }

    @PostMapping("/")
    public String create(@ModelAttribute("product") @RequestBody Product product, Model model) {
        try {
            product.setName(Validator.notEmpty(product.getName()));
            product.setDescription(Validator.notEmpty(product.getDescription()));
            // TODO - validate price
            //product.setPrice(Validator.notLessThan(product.getPrice(), Deci));
            product.setSkuCode(Validator.notEmpty(product.getSkuCode()));
            product.setStockLevel(Validator.nonNegative(product.getStockLevel(), "stock level"));

            String response = webClient
                    .post()
                    .uri("%s/api/products/".formatted(appSettings.getInventoryServiceUrl()))
                    .contentType(MediaType.APPLICATION_JSON)
                    .accept(MediaType.APPLICATION_JSON)
                    .bodyValue(product)
                    .retrieve()
                    .toEntity(String.class)
                    .block()
                    .getBody();

            product.setSkuCode("");

            model.addAttribute("info", "Successfully created product.");
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
        }

        model.addAttribute("product", product);
        model.addAttribute("page", "products");
        return "products-create.html";
    }
}