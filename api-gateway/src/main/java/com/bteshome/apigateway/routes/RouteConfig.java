package com.bteshome.apigateway.routes;

import com.bteshome.apigateway.common.AppSettings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
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
                .GET("/fallback", request -> ServerResponse
                        .status(HttpStatus.SERVICE_UNAVAILABLE)
                        .bodyValue("Service unavailable, please try again later."))
                .POST("/fallback", request -> ServerResponse
                        .status(HttpStatus.SERVICE_UNAVAILABLE)
                        .bodyValue("Service unavailable, please try again later."))
                .build();
    }

    @Bean
    public RouterFunction<ServerResponse> homeRoute() {
        return route()
                .GET("/", request -> ServerResponse
                        .ok()
                        .contentType(MediaType.TEXT_HTML)
                        .bodyValue(new ClassPathResource("_static2/" + appSettings.getIndexPage())))
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
    public RouteLocator ordersQueryRoute(RouteLocatorBuilder builder) {
        return builder.routes()
                .route("ordersQuery", r -> r
                        .path("/orders/query/**")
                        .and()
                        .method("GET")
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

    @Bean
    public RouteLocator ordersCreateRoute(RouteLocatorBuilder builder) {
        return builder.routes()
                .route("ordersCreate", r -> r
                        .path("/orders/create/")
                        .and()
                        .method("POST")
                        .filters(f -> f
                                .rewritePath("/orders/create/", "/api/orders/create/")
                                /*.circuitBreaker(config -> config
                                        .setName("orders")
                                        .setFallbackUri("forward:/fallback"))*/)
                        .uri(appSettings.getOrderServiceUrl()))
                .build();
    }
}
