package org.atlasapi.content.v2.serialization;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.atlasapi.content.v2.model.udt.Distribution;

import static org.junit.Assert.assertEquals;

import org.joda.time.DateTime;
import org.junit.Test;

public class DistributionSerializationTest {

    private DistributionSerialization serializer;

    public DistributionSerializationTest() {
        serializer = new DistributionSerialization();
    }

    @Test
    public void distributionSerializesandDeserializes() {
        org.atlasapi.entity.Distribution distribution = org.atlasapi.entity.Distribution
                .builder()
                .withDistributor("distributor")
                .withformat("format")
                .withReleaseDate(DateTime.now())
                .build();

        List<org.atlasapi.entity.Distribution> distributions = new ArrayList<>();
        distributions.add(distribution);

        Iterable<Distribution> serialized = serializer.serialize(distributions);

        for (Distribution serializedDistribution : serialized) {
            assertEquals(serializedDistribution.getDistributor(), distribution.getDistributor());
            assertEquals(serializedDistribution.getFormat(), distribution.getFormat());
            assertEquals(serializedDistribution.getReleaseDate(), distribution.getReleaseDate());
        }

        Iterable<org.atlasapi.entity.Distribution> deserialized = serializer.deserialize(serialized);

        for (org.atlasapi.entity.Distribution deserializedDistribution : deserialized) {
            assertEquals(deserializedDistribution.getDistributor(), distribution.getDistributor());
            assertEquals(deserializedDistribution.getFormat(), distribution.getFormat());
            assertEquals(deserializedDistribution.getReleaseDate(), distribution.getReleaseDate());
        }
    }
}