package org.atlasapi.content;

import org.atlasapi.entity.Id;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class IndexQueryParams {

    private final Optional<FuzzyQueryParams> fuzzyQueryParams;
    private final Optional<QueryOrdering> ordering;
    private final Optional<Id> regionId;
    private final Optional<Float> broadcastWeighting;
    private final Optional<Float> titleWeighting;
    private final Optional<List<List<InclusionExclusionId>>> topicFilterIds;
    private final Boolean containerAvailabilityFilter;
    private final Optional<Id> brandId;

    public IndexQueryParams(Optional<FuzzyQueryParams> fuzzyQueryParams, Optional<QueryOrdering> ordering,
            Optional<Id> regionId, Optional<Float> broadcastWeighting, Optional<Float> titleWeighting, Optional<List<List<InclusionExclusionId>>> topicFilterIds,
            Boolean containerAvailability, Optional<Id> brandId) {
        this.fuzzyQueryParams = checkNotNull(fuzzyQueryParams);
        this.ordering = checkNotNull(ordering);
        this.regionId = checkNotNull(regionId);
        this.broadcastWeighting = checkNotNull(broadcastWeighting);
        this.titleWeighting = checkNotNull(titleWeighting);
        this.topicFilterIds = checkNotNull(topicFilterIds);
        this.containerAvailabilityFilter = containerAvailability;
        this.brandId = checkNotNull(brandId);
    }

    public Optional<Id> getBrandId() {
        return brandId;
    }

    public Optional<FuzzyQueryParams> getFuzzyQueryParams() {
        return fuzzyQueryParams;
    }

    public Optional<QueryOrdering> getOrdering() {
        return ordering;
    }

    public Optional<Id> getRegionId() {
        return regionId;
    }

    public Optional<Float> getBroadcastWeighting() {
        return broadcastWeighting;
    }

    public Optional<Float> getTitleWeighting() {
        return titleWeighting;
    }

    public Optional<List<List<InclusionExclusionId>>> getTopicFilterIds() {
        return topicFilterIds;
    }

    public Boolean shouldFilterUnavailableContainers() {
        return containerAvailabilityFilter;
    }
}
