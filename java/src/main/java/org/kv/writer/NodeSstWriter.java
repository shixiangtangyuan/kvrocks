package org.kv.writer;

import org.kv.common.KVException;
import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileWriter;

import redis.clients.jedis.util.JedisClusterCRC16;

import java.io.File;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class NodeSstWriter implements AutoCloseable {

    private static final String PATH_SEPARATOR = "/";
    private static final String META_DIR = "metadata/";
    private static final int MAX_KEY_PREFIX_LENGTH = 2;
    private static final int SINGLE_KV_TYPE_VALUE_PREFIX_LENGTH = 1 + 8;
    private static final int REDIS_TYPE_STRING = 1;
    private static final byte METADATA_TTL_MASK = (byte) 0x80;
    private static final byte METADATA_TYPE_MASK = 0x01;
    private static final int MAX_SLOT = 16384;
    private static final int MIN_SLOT = 0;

    private final SstWriterConfig config;
    private final Options options;
    private final EnvOptions envOptions;
    private final SlotRange slotRange;
    private SstFileWriter metaWriter;
    private ByteBuffer defaultKeyBuffer = ByteBuffer.allocateDirect(16384);
    private ByteBuffer defaultValueBuffer = ByteBuffer.allocateDirect(16384 * 8);
    private int currentFileNumber = 0;
    private byte[] preFileLastKey;

    public NodeSstWriter(SlotRange slotRange, SstWriterConfig config) throws KVException {
        this.slotRange = slotRange;
        if (slotRange.start() < MIN_SLOT || slotRange.end() >= MAX_SLOT) {
            throw new IllegalArgumentException("Invalid slot: " + slotRange.toString());
        }
        this.config = config;
        if (config.getDataType() != DataType.STRING) {
            throw new IllegalArgumentException("now only support string type");
        }

        this.options = new Options();
        options.setCompressionType(config.getCompressionType());
        this.envOptions = new EnvOptions();
        String directoryPath =
                config.getOutputDir()
                        + PATH_SEPARATOR
                        + slotRange.toString()
                        + PATH_SEPARATOR
                        + META_DIR;
        File directory = new File(directoryPath);
        if (!directory.exists() && !directory.mkdirs()) {
            System.out.println("create directory failed :" + directory.getAbsolutePath());
        }

        start();
    }

    public static Comparator<byte[]> keyComparator() {
        return new Comparator<byte[]>() {
            @Override
            public int compare(byte[] o1, byte[] o2) {
                int slot1 = JedisClusterCRC16.getSlot(o1);
                int slot2 = JedisClusterCRC16.getSlot(o2);
                if (slot1 != slot2) {
                    return Integer.compare(slot1, slot2);
                }
                return compareByteArrays(o1, o2);
            }
        };
    }

    public static int compareByteArrays(byte[] array1, byte[] array2) {
        int minLength = Math.min(array1.length, array2.length);
        for (int i = 0; i < minLength; i++) {
            int diff = Byte.compare(array1[i], array2[i]);
            if (diff != 0) {
                return diff;
            }
        }
        return Integer.compare(array1.length, array2.length);
    }

    public SstFileWriter getMetaWriter() {
        return metaWriter;
    }

    public void start() throws KVException {
        metaWriter = new SstFileWriter(envOptions, options);
        try {
            metaWriter.open(generateFileName());
        } catch (RocksDBException e) {
            throw new KVException(e);
        }
    }

    public void roll() {
        finish();
        close();
        currentFileNumber++;
        start();
    }

    public void putString(byte[] key, byte[] value) {
        long expireAtMs = 0;
        if (config.getTTL() > 0) {
            long timestamp = System.currentTimeMillis();
            expireAtMs = timestamp + config.getTTL() * 1000;
        }

        putBytes(key, value, expireAtMs);
    }

    public void putStringWithExpireAt(byte[] key, byte[] value, long expireAtMs) {
        putBytes(key, value, expireAtMs);
    }

    public void putBytes(byte[] key, byte[] value, long expireAtMs) {
        try {
            if (config.getMaxFileBytes() > 0
                    && getMetaWriter().fileSize() >= config.getMaxFileBytes()) {

                Comparator<byte[]> comparator = keyComparator();
                if (comparator.compare(key, preFileLastKey) <= 0) {
                    throw new IllegalArgumentException(
                            "Keys must be added in strict ascending order");
                }
                roll();
            }
            preFileLastKey = key;
            ByteBuffer kb = getKeyBuffer(key.length + MAX_KEY_PREFIX_LENGTH);
            putSlotId(kb, key, true);
            ((Buffer) kb).flip();

            ByteBuffer vb = getValueBuffer(value.length + SINGLE_KV_TYPE_VALUE_PREFIX_LENGTH);
            putMetadata(vb, REDIS_TYPE_STRING, expireAtMs);
            vb.put(value);
            ((Buffer) vb).flip();
            metaWriter.put(kb, vb);
        } catch (RocksDBException e) {
            throw new KVException(e);
        }
    }

    private void putMetadata(ByteBuffer bb, int type, long expireAtMs) {
        byte flags = (byte) (METADATA_TYPE_MASK & type);
        if (expireAtMs > 0) {
            flags |= METADATA_TTL_MASK;
            bb.put(flags);
            bb.putLong(expireAtMs);
        } else {
            flags &= ~METADATA_TTL_MASK;
            bb.put(flags);
        }
    }

    private void putSlotId(ByteBuffer bb, byte[] key, boolean encodeSlotId) {
        if (encodeSlotId) {
            int slot = JedisClusterCRC16.getSlot(key);
            if (!slotRange.contains(slot)) {
                throw new IllegalArgumentException("Invalid slot: " + slot);
            }
            bb.putShort((short) slot);
        }
        bb.put(key);
    }

    public void finish() throws KVException {
        try {
            metaWriter.finish();
        } catch (RocksDBException e) {
            throw new KVException(e);
        }
    }

    private String generateFileName() {
        List<String> parts = new ArrayList<>();
        parts.add(
                config.getOutputDir()
                        + PATH_SEPARATOR
                        + slotRange.toString()
                        + PATH_SEPARATOR
                        + META_DIR);
        parts.add(currentFileNumber + ".sst");
        return String.join(PATH_SEPARATOR, parts);
    }

    private ByteBuffer getKeyBuffer(int size) {
        if (size > defaultKeyBuffer.capacity()) {
            return ByteBuffer.allocateDirect(size);
        }
        ((Buffer) defaultKeyBuffer).clear();
        return defaultKeyBuffer;
    }

    private ByteBuffer getValueBuffer(int size) {
        if (size > defaultValueBuffer.capacity()) {
            return ByteBuffer.allocateDirect(size);
        }
        ((Buffer) defaultValueBuffer).clear();
        return defaultValueBuffer;
    }

    @Override
    public void close() {
        metaWriter.close();
    }
}
