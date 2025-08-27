package org.kv.common;

public class KVException extends RuntimeException {
    public KVException(Throwable cause) {
        super(cause);
    }

    public KVException(String message) {
        super(message);
    }
}
