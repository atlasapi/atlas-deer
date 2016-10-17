package org.atlasapi.content.v2.serialization;

import java.util.ArrayList;
import java.util.List;

import org.atlasapi.content.v2.model.udt.Distribution;

public class DistributionSerialization {

    public Iterable<Distribution> serialize(Iterable<org.atlasapi.entity.Distribution> distributions) {
        List<Distribution> newDistributions = new ArrayList<>();
        for (org.atlasapi.entity.Distribution distribution : distributions) {
            newDistributions.add(serializeDistribution(distribution));
        }
        return newDistributions;
    }

    public Distribution serializeDistribution(org.atlasapi.entity.Distribution oldDistribution) {
        Distribution distribution = new Distribution();
        distribution.setDistributor(oldDistribution.getDistributor());
        distribution.setFormat(oldDistribution.getFormat());
        distribution.setReleaseDate(oldDistribution.getReleaseDate());
        return distribution;
    }

    public Iterable<org.atlasapi.entity.Distribution> deserialize(Iterable<Distribution> input) {
        List<org.atlasapi.entity.Distribution> distributions = new ArrayList<>();
        for (Distribution distribution : input) {
            distributions.add(deserializeDistribution(distribution));
        }
        return distributions;
    }

    public org.atlasapi.entity.Distribution deserializeDistribution(Distribution input) {
        return org.atlasapi.entity.Distribution.builder()
                .withReleaseDate(input.getReleaseDate())
                .withformat(input.getFormat())
                .withDistributor(input.getDistributor())
                .build();
    }
}
