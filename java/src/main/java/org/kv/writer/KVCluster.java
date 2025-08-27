package org.kv.writer;

import org.kv.common.KVException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.util.JedisClusterCRC16;

import java.io.Serializable;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KVCluster implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(KVCluster.class);
    private static final String SLOT_OUT_OF_RANGE = "Slot out of range.";
    private static final String SLOT_RANGE_NAME = "Group";

    private final Map<Integer, String> slotNodeMap = new LinkedHashMap<>();
    private final Map<Integer, SlotRange> rangeMap = new LinkedHashMap<>();
    private final String addr;
    private final int port;
    private final String userName;
    private final String passWord;
    private final SlotRange slotRange = new SlotRange(0, 16383);

    public KVCluster(String addr, int port, String userName, String passWord) throws KVException {
        this.addr = addr;
        this.port = port;
        this.userName = userName;
        this.passWord = passWord;
        connectKVCluster();
        slotNodeMap.forEach((k, v) -> rangeMap.put(k, parseRange(v)));
    }

    public int getPartitionId(byte[] key) {
        int slotId = getSlotId(key);
        if (!slotRange.contains(slotId)) {
            throw new KVException(SLOT_OUT_OF_RANGE + key);
        }
        return slotId;
    }

    private static SlotRange parseRange(String rangeStr) {
        String[] parts = rangeStr.split("[-_]");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid range format: " + rangeStr);
        }
        return new SlotRange(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
    }

    private void connectKVCluster() {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(10);
        poolConfig.setMaxIdle(5);
        poolConfig.setMinIdle(1);

        logger.info("addr:{}, prot:{}", addr, port);
        try (JedisPool jedisPool = new JedisPool(poolConfig, addr, port)) {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.auth(userName, passWord);
                String pingResponse = jedis.ping();
                logger.info("Connect succeeded: {}", pingResponse);
                String clusterNodesInfo = jedis.clusterNodes();
                List<String> ranges = parseKVClusterSlots(clusterNodesInfo);

                ranges.sort(
                        Comparator.comparingInt(
                                s -> {
                                    String[] parts = s.split("-");
                                    if (parts.length > 0) {
                                        try {
                                            return Integer.parseInt(parts[0]);
                                        } catch (NumberFormatException e) {
                                            throw new IllegalArgumentException(
                                                    "Invalid number format: " + parts[0], e);
                                        }
                                    } else {
                                        throw new IllegalArgumentException(
                                                "Invalid range format: " + s);
                                    }
                                }));

                for (int i = 0; i < ranges.size(); i++) {
                    slotNodeMap.put(i, ranges.get(i));
                }
                System.out.printf("|%-10s|%-20s|%n", "SlotID", "Slot");
                slotNodeMap.forEach((k, v) -> System.out.printf("|%-10s|%-20s|%n", k, v));
            }
        } catch (Exception e) {
            logger.error("Redis connect failed", e);
            throw new IllegalArgumentException("Redis connect failed" + e);
        }
    }

    private List<String> parseKVClusterSlots(String clusterNodesInfo) {
        List<String> ranges = new ArrayList<>();
        Pattern pattern = Pattern.compile("^(\\d+-\\d+)");
        String[] lines = clusterNodesInfo.split("\n");
        for (String line : lines) {
            line = line.trim();
            if (!line.isEmpty()) {
                Matcher matcher = pattern.matcher(line);
                if (matcher.find()) {
                    ranges.add(matcher.group(1));
                }
            }
        }

        return ranges;
    }

    public int getNumPartitions() {
        return slotNodeMap.size();
    }

    public static int getSlotId(String key) {
        return redis.clients.jedis.util.JedisClusterCRC16.getSlot(key);
    }

    public SlotRange getSlotRange(String key) {
        int number = JedisClusterCRC16.getSlot(key);
        for (Map.Entry<Integer, SlotRange> entry : rangeMap.entrySet()) {
            SlotRange range = entry.getValue();
            if (number >= range.start() && number <= range.end()) {
                return entry.getValue();
            }
        }

        throw new IllegalStateException("Invalid key:" + key);
    }

    public SlotRange getSlotRange(int partitionId) {
        if (rangeMap.isEmpty()) {
            throw new IllegalStateException("slotNodeMap is not initialized");
        }

        if (rangeMap.containsKey(partitionId)) {
            return rangeMap.get(partitionId);
        } else {
            throw new IllegalArgumentException("Invalid partition ID: " + partitionId);
        }
    }

    public List<SlotRange> getSlotRanges() {
        List<SlotRange> ranges = new ArrayList<>();
        for (Map.Entry<Integer, SlotRange> entry : rangeMap.entrySet()) {
            ranges.add(entry.getValue());
        }

        return ranges;
    }

    private int getSlotId(byte[] key) {
        int number = JedisClusterCRC16.getSlot(key);
        for (Map.Entry<Integer, SlotRange> entry : rangeMap.entrySet()) {
            SlotRange range = entry.getValue();
            if (number >= range.start() && number <= range.end()) {
                return entry.getKey();
            }
        }
        return -1;
    }
}
