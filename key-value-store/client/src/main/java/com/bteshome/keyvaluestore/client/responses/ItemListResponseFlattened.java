package com.bteshome.keyvaluestore.client.responses;

import lombok.*;

import java.util.List;
import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ItemListResponseFlattened<T> {
    private List<ItemResponse<T>> items;
    private Map<Integer, CursorPosition> cursorPositions;
    public boolean hasMore() {
        return cursorPositions
                .values()
                .stream()
                .anyMatch(CursorPosition::isHasMore);
    }
}