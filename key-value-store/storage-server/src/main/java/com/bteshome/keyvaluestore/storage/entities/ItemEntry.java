package com.bteshome.keyvaluestore.storage.entities;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public record ItemEntry(List<ItemValueVersion> valueVersions, Map<String, String> indexKeys) implements Serializable {
}
