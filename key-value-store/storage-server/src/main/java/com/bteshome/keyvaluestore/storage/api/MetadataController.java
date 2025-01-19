package com.bteshome.keyvaluestore.storage.api;

import com.bteshome.keyvaluestore.common.MetadataCache;
import com.bteshome.keyvaluestore.common.MetadataRefresher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/metadata")
@RequiredArgsConstructor
@Slf4j
public class MetadataController {
    @Autowired
    private MetadataRefresher metadataRefresher;

    @PostMapping("/notify-fetch/")
    public ResponseEntity<Long> fetch() {
        metadataRefresher.fetch();
        return ResponseEntity.ok(MetadataCache.getInstance().getLastFetchedVersion());
    }
}