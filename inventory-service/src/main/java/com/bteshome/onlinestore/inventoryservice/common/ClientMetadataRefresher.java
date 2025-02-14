package com.bteshome.onlinestore.inventoryservice.common;

import com.bteshome.keyvaluestore.client.ClientMetadataFetcher;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

@Component
public class ClientMetadataRefresher implements HandlerInterceptor {
    private long lastRefreshTime = 0;
    @Autowired
    private ClientMetadataFetcher clientMetadataFetcher;

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        if (System.currentTimeMillis() - lastRefreshTime > 10000) {
            clientMetadataFetcher.fetch();
            lastRefreshTime = System.currentTimeMillis();
        }

        return HandlerInterceptor.super.preHandle(request, response, handler);
    }
}
