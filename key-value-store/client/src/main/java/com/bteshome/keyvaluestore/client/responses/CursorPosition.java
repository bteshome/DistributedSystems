package com.bteshome.keyvaluestore.client.responses;

import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CursorPosition {
    private int partition;
    private String lastReadItemKey;
    private boolean hasMore;
}
