package com.bteshome.onlinestore.ui.controllers;

import com.bteshome.onlinestore.ui.dto.LineItem;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.util.WebUtils;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Controller
@RequestMapping("/cart")
@RequiredArgsConstructor
@Slf4j
public class CartController {
    @GetMapping("/")
    public String viewCart(Model model, HttpServletRequest httpServletRequest) {
        try {
            List<LineItem> lineItems = getLineItemsFromCookie(httpServletRequest);
            model.addAttribute("lineItems", lineItems);
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
        }

        model.addAttribute("page", "cart");
        return "cart.html";
    }

    @PostMapping("/")
    public ResponseEntity<?> addToCart(@RequestParam String skuCode,
                                       @RequestParam BigDecimal price,
                                        HttpServletRequest httpServletRequest,
                                        HttpServletResponse httpServletResponse) {
        try {
            List<String> cart = getCartFromCookie(httpServletRequest);
            LineItem lineItem = new LineItem(skuCode, 1, price);
            String cartItem = lineItem.toString();
            cart.add(cartItem);

            Cookie cartCookie = new Cookie("cart", String.join("|", cart));
            cartCookie.setMaxAge(60 * 60 * 24);
            cartCookie.setPath("/");
            httpServletResponse.addCookie(cartCookie);

            return ResponseEntity.ok(cart);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(e.getMessage());
        }
    }

    @PostMapping("/delete/")
    public ResponseEntity<?> deleteCartItem(@RequestParam String skuCode,
                                            @RequestParam int quantity,
                                             HttpServletRequest httpServletRequest,
                                             HttpServletResponse httpServletResponse) {
        try {
            LineItem lineItem = new LineItem(skuCode, quantity, BigDecimal.ZERO);
            List<LineItem> lineItems = getLineItemsFromCookie(httpServletRequest);
            lineItems.remove(lineItem);
            List<String> cart = lineItems.stream().map(LineItem::toString).toList();

            Cookie cartCookie = new Cookie("cart", String.join("|", cart));
            cartCookie.setMaxAge(lineItems.isEmpty() ? 0 : 60 * 60 * 24);
            cartCookie.setPath("/");
            httpServletResponse.addCookie(cartCookie);

            return ResponseEntity.ok(cart);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(e.getMessage());
        }
    }

    @PostMapping("/clear/")
    public ResponseEntity<?> clearCart(HttpServletResponse httpServletResponse) {
        try {
            Cookie cartCookie = new Cookie("cart", "");
            cartCookie.setMaxAge(0);
            cartCookie.setPath("/");
            httpServletResponse.addCookie(cartCookie);

            return ResponseEntity.ok(List.of());
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(e.getMessage());
        }
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
}