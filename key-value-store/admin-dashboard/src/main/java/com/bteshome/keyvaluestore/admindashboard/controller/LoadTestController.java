package com.bteshome.keyvaluestore.admindashboard.controller;

import com.bteshome.keyvaluestore.admindashboard.dto.LoadTestDto;
import com.bteshome.keyvaluestore.client.ItemWriter;
import com.bteshome.keyvaluestore.client.clientrequests.ItemWrite;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
        model.addAttribute("request", request);
        model.addAttribute("page", "load-test");
        return "load-test.html";
    }

    @PostMapping("/")
    public String test(@ModelAttribute("request") @RequestBody LoadTestDto request, Model model) {
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Random random = new Random();
            int requestIntervalMs = 1000 / request.getRequestsPerSecond();
            long totalNumRequests = request.getRequestsPerSecond() * request.getDuration().toSeconds();
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            List<ItemWrite<String>> itemWrites = new ArrayList<>();

            for (int i = 0; i < totalNumRequests; i++) {
                int randomNumber = random.nextInt(1, Integer.MAX_VALUE);
                String key = "key" + randomNumber;
                String value = "value" + randomNumber;
                ItemWrite<String> itemWrite = new ItemWrite<>(request.getTable(), key, value);
                itemWrites.add(itemWrite);
            }

            Instant startTime = Instant.now();

            for (ItemWrite<String> itemWrite : itemWrites) {
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> itemWriter.putString(itemWrite), executor);
                futures.add(future);
                try {
                    Thread.sleep(requestIntervalMs);
                } catch (InterruptedException ignored) {}
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

            Instant endTime = Instant.now();
            Duration timeSpent = Duration.between(startTime, endTime);
            long minutesSpent = timeSpent.toMinutes();
            long secondsSpent = timeSpent.minusMinutes(minutesSpent).toSeconds();
            long millisSpent = timeSpent.minusMinutes(minutesSpent).minusSeconds(secondsSpent).toMillis();

            String info = "%d requests sent. Actual time spent is %d minutes, %d seconds, %d milli seconds.".formatted(
                    totalNumRequests,
                    minutesSpent,
                    secondsSpent,
                    millisSpent
            );

            model.addAttribute("info", info);
            model.addAttribute("request", request);
            model.addAttribute("page", "load-test");
            return "load-test.html";
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
            model.addAttribute("request", request);
            model.addAttribute("page", "load-test");
            return "load-test.html";
        }
    }
}
