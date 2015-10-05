package org.atlasapi.content;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.atlasapi.entity.Id;
import org.atlasapi.query.common.Query;
import org.elasticsearch.common.lang3.StringUtils;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;


/***
    This is more or less a hack to allow parsing simple query params off of a HTTP request param map,
    found on a {@link Query}. It should be removed and replaced once a refactor of Deer's query
    parsing allows this sort of thing to be done easier and in a more general fashion.
 */
public class IndexQueryParser {

    private static final String NOT_OPERATOR = "!";

    private final NumberToShortStringCodec codec = SubstitutionTableNumberCodec.lowerCaseOnly();

    public IndexQueryParams parse(Query<?> query) {
        return new IndexQueryParams(
                titleQueryFrom(query),
                orderingFrom(query),
                regionIdFrom(query),
                broadcastWeightingFrom(query),
                titleWeightingFrom(query),
                topicIdsFrom(query),
                availabilityFilterFrom(query),
                brandIdFrom(query),
                actionableFilterParamsFrom(query),
                seriesIdFrom(query)
        );
    }

    private Optional<Id> seriesIdFrom(Query<?> query) {
        String seriesId = query.getContext().getRequest().getParameter("series.id");
        if (!Strings.isNullOrEmpty(seriesId)) {
            return Optional.of(Id.valueOf(codec.decode(seriesId)));
        }
        return Optional.empty();
    }

    @VisibleForTesting
    public Optional<Map<String, String>> actionableFilterParamsFrom(Query<?> query) {
        String[] params = (String[]) query.getContext().getRequest().getParameterMap().get("actionableFilterParameters");
        if (params == null || params.length == 0) {
            return Optional.empty();
        }
        String filters = params[0];
        List<String> splitFilters = Splitter.on(",").splitToList(filters);

        Splitter commaSplitter = Splitter.on(":");
        ImmutableMap.Builder<String, String> result = ImmutableMap.builder();
        for (String filter : splitFilters) {
            List<String> keyAndValue = commaSplitter.splitToList(filter);
            result.put(keyAndValue.get(0), keyAndValue.get(1));
        }
        return Optional.of(result.build());
    }

    private Optional<Id> brandIdFrom(Query<?> query) {
        String episodeBrandId = query.getContext().getRequest().getParameter("episode.brand.id");
        if (!Strings.isNullOrEmpty(episodeBrandId)) {
            return Optional.of(Id.valueOf(codec.decode(episodeBrandId)));
        }
        String brandId = query.getContext().getRequest().getParameter("brand.id");
        if (!Strings.isNullOrEmpty(brandId)) {
            return Optional.of(Id.valueOf(codec.decode(brandId)));
        }
        return Optional.empty();
    }

    private Boolean availabilityFilterFrom(Query<?> query) {
        String available = query.getContext().getRequest().getParameter("available");
        String brandSeriesAvailable = query.getContext().getRequest().getParameter("brand.series.available");
        return !Strings.isNullOrEmpty(available) || !Strings.isNullOrEmpty(brandSeriesAvailable);
    }

    private Optional<List<List<InclusionExclusionId>>> topicIdsFrom(Query<?> query) {
        String topicIds = query.getContext().getRequest().getParameter("tags.topic.id");
        if (Strings.isNullOrEmpty(topicIds)) {
            return Optional.empty();
        }
        ImmutableList.Builder<List<InclusionExclusionId>> builder = ImmutableList.builder();
        Splitter commaSplitter = Splitter.on(",");
        Splitter carretSplitter = Splitter.on("^");
        for (String idCsv : commaSplitter.splitToList(topicIds)) {
            builder.add(Lists.transform(carretSplitter.splitToList(idCsv), this::parseInclusiveExclusiveId));
        }
        return Optional.of(builder.build());
    }

    private InclusionExclusionId parseInclusiveExclusiveId(String id) {
        if (StringUtils.startsWith(id, NOT_OPERATOR)) {
            return InclusionExclusionId.valueOf(
                    Id.valueOf(codec.decode(StringUtils.removeStart(id, NOT_OPERATOR))), Boolean.FALSE
            );
        }
        return InclusionExclusionId.valueOf(Id.valueOf(codec.decode(id)), Boolean.TRUE);
    }

    private Optional<Float> broadcastWeightingFrom(Query<?> query) {
        String broadcastWeight = query.getContext().getRequest().getParameter("broadcastWeight");
        if (Strings.isNullOrEmpty(broadcastWeight)) {
            return Optional.empty();
        }
        return Optional.of(Float.parseFloat(broadcastWeight));
    }

    private Optional<Float> titleWeightingFrom(Query<?> query) {
        String titleWeight = query.getContext().getRequest().getParameter("titleWeight");
        if (Strings.isNullOrEmpty(titleWeight)) {
            return Optional.empty();
        }
        return Optional.of(Float.parseFloat(titleWeight));
    }

    private Optional<Id> regionIdFrom(Query<?> query) {
        String stringRegionId = query.getContext().getRequest().getParameter("region");
        if (Strings.isNullOrEmpty(stringRegionId)) {
            return Optional.empty();
        }
        return Optional.of(Id.valueOf(codec.decode(stringRegionId)));
    }

    private Optional<QueryOrdering> orderingFrom(Query<?> query) {
        Map params = query.getContext().getRequest().getParameterMap();
        if (!params.containsKey("order_by")) {
            return Optional.empty();
        }
        String[] orderByVals = ((String[]) params.get("order_by"));
        if (orderByVals.length > 1) {
            throw new IllegalArgumentException("Cannot specify multiple order_by values");
        }
        if (orderByVals.length == 0) {
            return Optional.empty();
        }
        return Optional.ofNullable(QueryOrdering.fromOrderBy(orderByVals[0]));
    }

    private Optional<FuzzyQueryParams> titleQueryFrom(Query<?> query) {
        Optional<String> fuzzySearchString = getFuzzySearchString(query);
        if (!fuzzySearchString.isPresent()) {
            return Optional.empty();
        }
        return Optional.ofNullable(new FuzzyQueryParams(fuzzySearchString.get(), getTitleBoostFrom(query)));
    }

    private Optional<Float> getTitleBoostFrom(Query<?> query) {
        Map params = query.getContext().getRequest().getParameterMap();
        Object param = params.get("title_boost");
        if (param == null) {
            return Optional.empty();
        }
        String[] titleBoost = (String[]) param;
        if (titleBoost.length > 1) {
            throw new IllegalArgumentException("Title boost param (titleBoost) has been specified more than once");
        }
        if (titleBoost.length == 0) {
            return Optional.empty();
        }
        return Optional.ofNullable(Float.parseFloat(titleBoost[0]));
    }

    private Optional<String> getFuzzySearchString(Query<?> query) {
        Map params = query.getContext().getRequest().getParameterMap();
        if (!params.containsKey("q")) {
            return Optional.empty();
        }
        String[] searchParam = ((String[]) params.get("q"));
        if (searchParam.length > 1) {
            throw new IllegalArgumentException("Search param (q) has been specified more than once");
        }
        if (searchParam[0] == null) {
            return Optional.empty();
        }
        return Optional.of(searchParam[0]);
    }
}
