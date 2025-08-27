package org.kv.writer;

import java.io.Serializable;
import java.util.Comparator;

public class CustomComparator implements Comparator<CustomKey>, Serializable {
    @Override
    public int compare(CustomKey o1, CustomKey o2) {
        if (o1 == null || o2 == null) {
            throw new IllegalArgumentException("compare key is null");
        }

        int slotCompare = Integer.compare(o1.slot, o2.slot);
        if (slotCompare != 0) {
            return slotCompare;
        }

        return o1.orgKey.compareTo(o2.orgKey);
    }
}
