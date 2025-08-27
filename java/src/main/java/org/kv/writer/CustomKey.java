package org.kv.writer;

import java.io.Serializable;

public class CustomKey implements Serializable {
    public Integer slot;
    public String orgKey;

    public CustomKey(Integer slot, String orgKey) {
        if (slot < 0 || slot > 16383) {
            throw new IllegalArgumentException("Invalid slot value");
        }
        this.slot = slot;
        this.orgKey = orgKey;
    }

    public Integer getSlot() {
        return this.slot;
    }

    public String getKey() {
        return this.orgKey;
    }
}
