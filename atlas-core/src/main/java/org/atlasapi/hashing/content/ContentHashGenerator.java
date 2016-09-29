package org.atlasapi.hashing.content;

import java.util.Optional;
import java.util.UUID;

import org.atlasapi.content.Content;
import org.atlasapi.hashing.HashGenerator;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This class is intended to act as an adaptor between the existing {@link ContentHasher} interface
 * which our persistence stores expect and the new {@link HashGenerator}.
 */
public class ContentHashGenerator implements ContentHasher {

    private static final Logger log = LoggerFactory.getLogger(ContentHashGenerator.class);

    private final HashGenerator hashGenerator;
    private final Meter hashGeneratedMeter;
    private final Meter hashGenerationFailedMeter;

    private ContentHashGenerator(
            HashGenerator hashGenerator,
            String metricPrefix,
            MetricRegistry metricRegistry
    ) {
        this.hashGenerator = checkNotNull(hashGenerator);

        this.hashGeneratedMeter = metricRegistry.meter(
                metricPrefix + "contentHashGenerator.meter.generated"
        );
        this.hashGenerationFailedMeter = metricRegistry.meter(
                metricPrefix + "contentHashGenerator.meter.failed"
        );
    }

    public static ContentHashGenerator create(
            HashGenerator hashGenerator,
            String metricPrefix,
            MetricRegistry metricRegistry
    ) {
        return new ContentHashGenerator(hashGenerator, metricPrefix, metricRegistry);
    }

    @Override
    public String hash(Content content) {
        Optional<String> hash = hashGenerator.hash(content);

        if (hash.isPresent()) {
            hashGeneratedMeter.mark();
            return hash.get();
        }

        hashGenerationFailedMeter.mark();
        log.warn("Failed to generate hash for id={}, class={}",
                content.getId(), content.getClass().getCanonicalName());

        // If we have failed to generate a hash then return a random value. This guarantees that
        // we will conservatively find the hashes don't match and therefore assume the content has
        // changed and write it to the DB. This might increase the write load by doing unnecessary
        // writes, but avoids incorrectly missing a write that should have happened.
        return UUID.randomUUID().toString();
    }
}
