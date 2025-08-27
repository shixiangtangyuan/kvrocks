package org.kv.writer;

import org.apache.spark.Partitioner;

public class CustomPartitioner extends Partitioner {
    private static final int REDIS_CLUSTER_SLOTS = 16384;

    @Override
    public int numPartitions() {
        return REDIS_CLUSTER_SLOTS;
    }

    @Override
    public int getPartition(Object key) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        if (!(key instanceof CustomKey)) {
            throw new IllegalArgumentException("Key must be of type CustomKey");
        }

        CustomKey customKey = (CustomKey) key;
        int slot = customKey.getSlot();

        // 槽位有效性校验
        if (slot < 0 || slot >= REDIS_CLUSTER_SLOTS) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid slot %d. Slot must be in [0, %d]",
                            slot, REDIS_CLUSTER_SLOTS - 1));
        }

        return slot;
    }
}
