package org.kv.writer;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.kv.common.KVException;
import org.rocksdb.CompressionType;

import redis.clients.jedis.util.JedisClusterCRC16;

import java.util.*;
import java.util.Map;
import java.util.TreeMap;

public class Main {
    public static void main(String[] args) {
        // KVCluster cluster = new KVCluster("cdzn9.cluster.yalin.dev.kv.shopee.io", 46093,
        // "r_hpej1", "sCNDUZy2A3");
        SstWriterConfig config = new SstWriterConfig();
        config.setOutputDir("./sst_output");
        config.setCompressionType(CompressionType.LZ4_COMPRESSION);
        config.setTTL(3600);
        // 创建自定义比较器的 TreeMap
        Map<byte[], byte[]> sortedMap = new TreeMap<>(NodeSstWriter.keyComparator());

        Map<SlotRange, NodeSstWriter> sst_writers = new HashMap<>();
        // 初始化sstwriter
        SlotRange range1 = new SlotRange(0, 4095);
        SlotRange range2 = new SlotRange(4096, 8191);
        SlotRange range3 = new SlotRange(8192, 12287);
        SlotRange range4 = new SlotRange(12288, 16383);
        sst_writers.put(range1, (new NodeSstWriter(range1, config)));
        sst_writers.put(range2, (new NodeSstWriter(range2, config)));
        sst_writers.put(range3, (new NodeSstWriter(range3, config)));
        sst_writers.put(range4, (new NodeSstWriter(range4, config)));

        // 设置key个数和value的大小
        long key_num = 1000L;
        int value_size = 1024;
        String key_pre = "sst_test:";
        String value_pre = new String(new char[value_size]).replace('\0', 'a');

        for (int i = 0; i < key_num; i++) {
            String key1 = key_pre + i;
            String value1 = value_pre + i;
            sortedMap.put(key1.getBytes(UTF_8), value1.getBytes(UTF_8));
        }

        // 遍历输出（按槽位和键排序）
        sortedMap.forEach(
                (key, value) -> {
                    int slot = JedisClusterCRC16.getSlot(key);
                });

        try {
            // 按照排序后的顺序写入键值对
            for (Map.Entry<byte[], byte[]> entry : sortedMap.entrySet()) {
                int slot = JedisClusterCRC16.getSlot(entry.getKey());
                for (Map.Entry<SlotRange, NodeSstWriter> sst_writer : sst_writers.entrySet()) {
                    if (sst_writer.getKey().contains(slot)) {
                        sst_writer.getValue().putString(entry.getKey(), entry.getValue());
                    }
                }
            }
            sst_writers.forEach(
                    (key, value) -> {
                        value.finish();
                    });

            System.out.println("SST文件生成成功！");
        } catch (KVException e) {
            System.err.println("写入过程中发生错误：" + e.getMessage());
            e.printStackTrace();
        }
    }
}
