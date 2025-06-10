package org.integratedmodelling.klab.nifi;

import java.util.HashMap;
import java.util.Map;

public class EventData {
    private final Object payload;
    private final Map<String, String> attributes;
    private final long timestamp;

    public EventData(Object payload, Map<String, String> attributes) {
        this.payload = payload;
    this.attributes = new HashMap<>(attributes);
        this.timestamp = System.currentTimeMillis();
        this.attributes.put("event.timestamp", String.valueOf(timestamp));
    }

    // Getters
    public Object getPayload() { return payload; }
    public Map<String, String> getAttributes() { return attributes; }
    public long getTimestamp() { return timestamp; }
}

