package io.jonasg.kawa.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.Base64;

public final class HashedPassword {

    private static final String PREFIX = "pbkdf2-sha256$";
    private static final int ITERATIONS = 120_000;
    private static final int KEY_LENGTH = 256;

    private final String encoded;

    public HashedPassword(String password, String salt) {
        this.encoded = password.startsWith(PREFIX) ? password : encode(password, salt);
    }

    private HashedPassword(String encoded) {
        this.encoded = encoded;
    }

    @JsonCreator
    public static HashedPassword fromEncoded(String encoded) {
        if (encoded == null || encoded.isBlank()) {
            throw new IllegalArgumentException("hashed password must not be null or blank");
        }
        return new HashedPassword(encoded);
    }

    @JsonValue
    public String encoded() {
        return encoded;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof HashedPassword password
                && MessageDigest.isEqual(
                encoded.getBytes(StandardCharsets.UTF_8),
                password.encoded.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public int hashCode() {
        return encoded.hashCode();
    }

    private static String encode(String password, String salt) {
        byte[] derived = derive(password, salt);
        return PREFIX + ITERATIONS + "$" + Base64.getEncoder().encodeToString(derived);
    }

    private static byte[] derive(String password, String salt) {
        try {
            var specification = new PBEKeySpec(
                    password.toCharArray(),
                    saltBytes(salt),
                    ITERATIONS,
                    KEY_LENGTH);
            return SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256")
                    .generateSecret(specification)
                    .getEncoded();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("PBKDF2 password hashing is unavailable", e);
        }
    }

    private static byte[] saltBytes(String salt) {
        return salt == null || salt.isEmpty()
                ? new byte[]{0}
                : salt.getBytes(StandardCharsets.UTF_8);
    }
}
