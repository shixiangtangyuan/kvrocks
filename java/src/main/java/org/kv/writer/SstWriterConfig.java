package org.kv.writer;

import org.rocksdb.CompressionType;

public class SstWriterConfig {
    private String metaColumnFamilyName = "metadata";
    private CompressionType compressionType = CompressionType.LZ4_COMPRESSION;
    private String outputDir;
    private long maxFileBytes = 1024L * 1024 * 128;
    private long ttl = 0L; // unit second
    private DataType type = DataType.STRING;

    public String getMetaColumnFamilyName() {
        return metaColumnFamilyName;
    }

    public void setMetaColumnFamilyName(String metaColumnFamilyName) {
        this.metaColumnFamilyName = metaColumnFamilyName;
    }

    public CompressionType getCompressionType() {
        return compressionType;
    }

    public void setCompressionType(CompressionType compressionType) {
        this.compressionType = compressionType;
    }

    public String getOutputDir() {
        return outputDir;
    }

    public void setOutputDir(String outputDir) {
        this.outputDir = outputDir;
    }

    public long getMaxFileBytes() {
        return maxFileBytes;
    }

    public void setMaxFileBytes(long maxFileBytes) {
        this.maxFileBytes = maxFileBytes;
    }

    public void setTTL(long ttl) {
        this.ttl = ttl;
    }

    public long getTTL() {
        return ttl;
    }

    public void setDataType(DataType type) {
        this.type = type;
    }

    public DataType getDataType() {
        return type;
    }
}
