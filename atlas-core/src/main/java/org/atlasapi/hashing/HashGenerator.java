package org.atlasapi.hashing;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class HashGenerator {

    private static final Logger log = LoggerFactory.getLogger(HashGenerator.class);

    private static final String HASHING_ALGORITHM = "SHA-256";

    private final HashValueExtractor extractor;

    private HashGenerator(HashValueExtractor hashValueExtractor) {
        this.extractor = checkNotNull(hashValueExtractor);
    }

    public static HashGenerator create() {
        return new HashGenerator(HashValueExtractor.create());
    }

    @VisibleForTesting
    static HashGenerator create(HashValueExtractor extractor) {
        return new HashGenerator(extractor);
    }

    public Optional<String> hash(Hashable hashable) {
        Optional<String> value = extractor.getValueToHash(hashable);

        if (!value.isPresent()) {
            return Optional.empty();
        }

        try {
            return Optional.of(hash(value.get()));
        } catch (Exception e) {
            log.warn("Failed to generate hash for object of type {}",
                    hashable.getClass().getCanonicalName(), e);
            return Optional.empty();
        }
    }

    private String hash(String value) {
        MessageDigest messageDigest = getMessageDigest();
        messageDigest.update(value.getBytes(StandardCharsets.UTF_16));
        byte[] digest = messageDigest.digest();

        return Base64.encodeBase64URLSafeString(digest);
    }

    private MessageDigest getMessageDigest() {
        MessageDigest messageDigest;
        try {
            messageDigest = MessageDigest.getInstance(HASHING_ALGORITHM);
        } catch (NoSuchAlgorithmException e) {
            throw Throwables.propagate(e);
        }
        return messageDigest;
    }
}
