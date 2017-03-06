package org.atlasapi.content;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.atlasapi.entity.Id;

import static com.google.common.base.Preconditions.checkNotNull;

public class IndexQueryParams {

    private final Optional<FuzzyQueryParams> fuzzyQueryParams;
    private final Optional<QueryOrdering> ordering;
    private final Optional<Id> regionId;
    private final Optional<Float> broadcastWeighting;
    private final Optional<Float> titleWeighting;
    private final Optional<List<List<InclusionExclusionId>>> topicFilterIds;
    private final Boolean availabilityFilter;
    private final Optional<Id> brandId;
    private final Optional<Map<String, String>> actionableFilterParams;
    private final Optional<Id> seriesId;

    public IndexQueryParams(Optional<FuzzyQueryParams> fuzzyQueryParams,
            Optional<QueryOrdering> ordering,
            Optional<Id> regionId, Optional<Float> broadcastWeighting,
            Optional<Float> titleWeighting,
            Optional<List<List<InclusionExclusionId>>> topicFilterIds,
            Boolean containerAvailability, Optional<Id> brandId,
            Optional<Map<String, String>> actionableFilterParams, Optional<Id> seriesId) {
        this.fuzzyQueryParams = checkNotNull(fuzzyQueryParams);
        this.ordering = checkNotNull(ordering);
        this.regionId = checkNotNull(regionId);
        this.broadcastWeighting = checkNotNull(broadcastWeighting);
        this.titleWeighting = checkNotNull(titleWeighting);
        this.topicFilterIds = checkNotNull(topicFilterIds);
        this.availabilityFilter = containerAvailability;
        this.brandId = checkNotNull(brandId);
        this.actionableFilterParams = checkNotNull(actionableFilterParams);
        this.seriesId = checkNotNull(seriesId);
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
        return availabilityFilter;
    }

    public Optional<Map<String, String>> getActionableFilterParams() {
        return actionableFilterParams;
    }

    public Optional<Id> getSeriesId() {
        return seriesId;
    }
}
