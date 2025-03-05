package com.bteshome.apigateway.ratelimiter;

import com.bteshome.apigateway.common.AppSettings;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

@Slf4j
@Component
@ConditionalOnProperty(name = "rate-limiter-disabled", havingValue = "false", matchIfMissing = true)
@Order(Ordered.HIGHEST_PRECEDENCE)
public class RateLimiterConfig implements WebFilter {
    @Autowired
    private RateLimiter limiter;
    @Autowired
    private AppSettings appSettings;
    private static final String API_NAME_API_GATEWAY = "api-gateway";
    private static final String API_NAME_INVENTORY = "inventory";
    private static final String API_NAME_ORDERS = "orders";
    private static final String API_NAME_ORDERING_UI = "ordering-ui";

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String path = exchange.getRequest().getPath().toString();
        String api = "";

        if (path.equals("/")) {
            api = API_NAME_API_GATEWAY;
        } else {
            String pathFirstPart = path.split("/")[1];
            switch (pathFirstPart) {
                case "favicon.ico", "public", "actuator" -> api = API_NAME_API_GATEWAY;
                case "inventory" -> api = API_NAME_INVENTORY;
                case "orders" -> api = API_NAME_ORDERS;
                case "ordering-ui" -> api = API_NAME_ORDERING_UI;
            }
        }

        if (api.isEmpty()) {
            exchange.getResponse().setStatusCode(HttpStatus.NOT_FOUND);
            return exchange.getResponse().setComplete();
        }

        String xForwardedForHeader = exchange.getRequest().getHeaders().getFirst("X-Forwarded-For");
        String clientIp = (xForwardedForHeader != null) ?
                xForwardedForHeader.split(",")[0].trim() :
                exchange.getRequest().getRemoteAddress().getAddress().getHostAddress();

        if (limiter.tryAcquire(api, clientIp.toString())) {
            log.debug("RateLimiter allowed caller: {}.", clientIp);
            return chain.filter(exchange);
        } else {
            log.debug("RateLimiter blocked caller: {}.", clientIp);
            exchange.getResponse().setStatusCode(HttpStatus.TOO_MANY_REQUESTS);
            return exchange.getResponse().setComplete();
        }
    }
}