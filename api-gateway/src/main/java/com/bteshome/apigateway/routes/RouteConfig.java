package com.bteshome.apigateway.routes;

import com.bteshome.apigateway.common.AppSettings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;

import static org.springframework.web.reactive.function.server.RouterFunctions.route;

@Configuration
public class RouteConfig {
    @Autowired
    private AppSettings appSettings;

    @Bean
    public RouterFunction<ServerResponse> fallbackRoute() {
        return route()
            .GET ("/fallback", request -> ServerResponse
                .status(HttpStatus.SERVICE_UNAVAILABLE)
                .bodyValue("Service unavailable, please try again later."))
            .POST("/fallback", request -> ServerResponse
                    .status(HttpStatus.SERVICE_UNAVAILABLE)
                    .bodyValue("Service unavailable, please try again later."))
            .build();
    }

    @Bean
    public RouteLocator productsRoute(RouteLocatorBuilder builder) {
        return builder.routes()
                .route("products", r -> r
                        .path("/inventory/products/")
                        .and()
                        .method("GET")
                        .filters(f -> f
                                        .rewritePath("/inventory/products/", "/api/products/")
                                        /*.circuitBreaker(config -> config
                                                .setName("inventory")
                                                .setFallbackUri("forward:/fallback"))*/
                                /*.retry(config -> config
                                        .setRetries(3)
                                        .setMethods(HttpMethod.GET)
                                        .setStatuses(HttpStatus.BAD_GATEWAY, HttpStatus.GATEWAY_TIMEOUT)
                                        .setBackoff(Duration.ofMillis(100), Duration.ofMillis(1000), 2, true))*/)
                        .uri(appSettings.getInventoryServiceUrl()))
                .build();
    }

    @Bean
    public RouteLocator ordersRoute(RouteLocatorBuilder builder) {
        return builder.routes()
                .route("orders", r -> r
                        .path("/orders/query/**")
                        .filters(f -> f
                                        .rewritePath("/orders/?(?<remaining>.*)", "/api/orders/${remaining}")
                                        /*.circuitBreaker(config -> config
                                                .setName("orders")
                                                .setFallbackUri("forward:/fallback"))*/
                                /*.retry(config -> config
                                        .setRetries(3)
                                        .setMethods(HttpMethod.GET)
                                        .setStatuses(HttpStatus.BAD_GATEWAY, HttpStatus.GATEWAY_TIMEOUT)
                                        .setBackoff(Duration.ofMillis(100), Duration.ofMillis(1000), 2, true))*/)
                        .uri(appSettings.getOrderServiceUrl()))
                .build();
    }
}
