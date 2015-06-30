package org.atlasapi.query.common;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import org.atlasapi.content.IndexQueryParams;
import org.atlasapi.content.QueryOrdering;
import org.atlasapi.content.FuzzyQueryParams;
import org.atlasapi.entity.Id;

import java.util.Map;
import java.util.Optional;


/***
    This is more or less a hack to allow parsing simple query params off of a HTTP request param map,
    found on a {@link Query}. It should be removed and replaced once a refactor of Deer's query
    parsing allows this sort of thing to be done easier and in a more general fashion.
 */
public class IndexQueryParser {

    private final NumberToShortStringCodec codec = SubstitutionTableNumberCodec.lowerCaseOnly();

    public IndexQueryParams parse(Query<?> query) throws QueryParseException {
        return new IndexQueryParams(
                titleQueryFrom(query),
                orderingFrom(query),
                regionIdFrom(query),
                broadcastWeightingFrom(query),
                titleWeightingFrom(query)
        );

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

    private Optional<QueryOrdering> orderingFrom(Query<?> query) throws QueryParseException {
        Map params = query.getContext().getRequest().getParameterMap();
        if (!params.containsKey("order_by")) {
            return Optional.empty();
        }
        String[] orderByVals = ((String[]) params.get("order_by"));
        if (orderByVals.length > 1) {
            throw new QueryParseException("Cannot specify multiple order_by values");
        }
        if (orderByVals.length == 0) {
            return Optional.empty();
        }
        return Optional.ofNullable(QueryOrdering.fromOrderBy(orderByVals[0]));
    }

    private Optional<FuzzyQueryParams> titleQueryFrom(Query<?> query) throws QueryParseException {
        Optional<String> fuzzySearchString = getFuzzySearchString(query);
        if (!fuzzySearchString.isPresent()) {
            return Optional.empty();
        }
        return Optional.ofNullable(new FuzzyQueryParams(fuzzySearchString.get(), getTitleBoostFrom(query)));
    }

    private Optional<Float> getTitleBoostFrom(Query<?> query) throws QueryParseException {
        Map params = query.getContext().getRequest().getParameterMap();
        Object param = params.get("title_boost");
        if (param == null) {
            return Optional.empty();
        }
        String[] titleBoost = (String[]) param;
        if (titleBoost.length > 1) {
            throw new QueryParseException("Title boost param (titleBoost) has been specified more than once");
        }
        if (titleBoost.length == 0) {
            return Optional.empty();
        }
        return Optional.ofNullable(Float.parseFloat(titleBoost[0]));
    }

    private Optional<String> getFuzzySearchString(Query<?> query) throws QueryParseException {
        Map params = query.getContext().getRequest().getParameterMap();
        if (!params.containsKey("q")) {
            return Optional.empty();
        }
        String[] searchParam = ((String[]) params.get("q"));
        if (searchParam.length > 1) {
            throw new QueryParseException("Search param (q) has been specified more than once");
        }
        if (searchParam[0] == null) {
            return Optional.empty();
        }
        return Optional.of(searchParam[0]);
    }
}
