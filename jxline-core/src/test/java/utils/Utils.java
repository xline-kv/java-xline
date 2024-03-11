package utils;

import io.etcd.jetcd.ByteSequence;

import java.nio.charset.StandardCharsets;

public class Utils {

    public static ByteSequence bytesOf(final String string) {
        return ByteSequence.from(string, StandardCharsets.UTF_8);
    }

    public static String randomString() {
        return java.util.UUID.randomUUID().toString();
    }
}
