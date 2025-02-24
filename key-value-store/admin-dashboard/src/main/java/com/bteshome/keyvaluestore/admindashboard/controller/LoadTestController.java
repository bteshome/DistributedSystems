package com.bteshome.keyvaluestore.admindashboard.controller;

import com.bteshome.keyvaluestore.admindashboard.common.AdminDashboardException;
import com.bteshome.keyvaluestore.admindashboard.dto.LoadTestDto;
import com.bteshome.keyvaluestore.client.writers.ItemWriter;
import com.bteshome.keyvaluestore.client.clientrequests.ItemWrite;
import com.bteshome.keyvaluestore.client.requests.AckType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

@Controller
@RequestMapping("/items/load-test/")
@RequiredArgsConstructor
@Slf4j
public class LoadTestController {
    @Autowired
    ItemWriter itemWriter;

    @GetMapping("/")
    public String test(Model model) {
        LoadTestDto request = new LoadTestDto();
        request.setTable("table1");
        request.setRequestsPerSecond(10);
        request.setDuration(Duration.ofSeconds(1));
        request.setAck(AckType.NONE);
        request.setMaxRetries(0);
        model.addAttribute("request", request);
        model.addAttribute("page", "load-test");
        return "load-test.html";
    }

    @PostMapping("/")
    public String test(@ModelAttribute("request") @RequestBody LoadTestDto request, Model model) {
        Random random = new Random();
        int requestIntervalMs = 1000 / request.getRequestsPerSecond();
        long totalNumRequests = request.getRequestsPerSecond() * request.getDuration().toSeconds();
        List<ItemWrite<byte[]>> itemWrites = new ArrayList<>();

        for (int i = 0; i < totalNumRequests; i++) {
            int randomNumber = random.nextInt(1, Integer.MAX_VALUE);
            String key = "key" + randomNumber;
            String value = "value" + randomNumber;
            ItemWrite<byte[]> itemWrite = new ItemWrite<>(
                    request.getTable(),
                    key,
                    value.getBytes(),
                    null,
                    request.getAck(),
                    request.getMaxRetries(),
                    null);
            itemWrites.add(itemWrite);
        }

        Instant startTime = Instant.now();
        AtomicInteger succeededRequestsCount = new AtomicInteger(0);

        try {
            Flux.fromIterable(itemWrites)
                    .zipWith(Flux.interval(Duration.ofMillis(requestIntervalMs)))
                    .map(Tuple2::getT1)
                    .onBackpressureDrop()
                    .flatMap(itemWrite -> itemWriter.putBytes(itemWrite)
                            .doOnSuccess(response -> {
                                if (response.getHttpStatusCode() == 200)
                                    succeededRequestsCount.incrementAndGet();
                            })
                            .subscribeOn(Schedulers.boundedElastic())
                    )
                    .onErrorMap(e -> new AdminDashboardException(e.getClass().getName() + " - " + e.getMessage(), e))
                    .blockLast();
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
        }

        Instant endTime = Instant.now();
        Duration timeSpent = Duration.between(startTime, endTime);
        long minutesSpent = timeSpent.toMinutes();
        long secondsSpent = timeSpent.minusMinutes(minutesSpent).toSeconds();
        long millisSpent = timeSpent.minusMinutes(minutesSpent).minusSeconds(secondsSpent).toMillis();

        String info = "Successfully sent %d/%d requests. Actual time spent is %d minutes, %d seconds, %d milli seconds.".formatted(
                succeededRequestsCount.get(),
                totalNumRequests,
                minutesSpent,
                secondsSpent,
                millisSpent
        );

        model.addAttribute("request", request);
        model.addAttribute("page", "load-test");
        model.addAttribute("info", info);
        return "load-test.html";
    }
}
