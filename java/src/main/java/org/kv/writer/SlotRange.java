package org.kv.writer;

import java.io.Serializable;

public final class SlotRange implements Serializable {
    private final int start;
    private final int end;

    public SlotRange(int start, int end) {
        if (end < start) {
            throw new IllegalArgumentException("end small than start");
        }
        this.start = start;
        this.end = end;
    }

    public int start() {
        return start;
    }

    public int end() {
        return end;
    }

    public boolean contains(int value) {
        return value >= start && value <= end;
    }

    public String toString() {
        return start + "_" + end;
    }
}
