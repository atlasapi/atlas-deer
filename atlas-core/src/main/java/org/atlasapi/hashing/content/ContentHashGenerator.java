package org.atlasapi.hashing.content;

import java.util.Optional;
import java.util.UUID;

import org.atlasapi.content.Content;
import org.atlasapi.hashing.HashGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentHashGenerator implements ContentHasher {

    private static final Logger log = LoggerFactory.getLogger(ContentHashGenerator.class);

    private final HashGenerator hashGenerator;

    private ContentHashGenerator(HashGenerator hashGenerator) {
        this.hashGenerator = checkNotNull(hashGenerator);
    }

    public static ContentHashGenerator create(HashGenerator hashGenerator) {
        return new ContentHashGenerator(hashGenerator);
    }

    @Override
    public String hash(Content content) {
        Optional<String> hash = hashGenerator.hash(content);

        if (hash.isPresent()) {
            return hash.get();
        }

        log.warn("Failed to generate hash for id={}, class={}",
                content.getId(), content.getClass().getCanonicalName());
        return UUID.randomUUID().toString();
    }
}
