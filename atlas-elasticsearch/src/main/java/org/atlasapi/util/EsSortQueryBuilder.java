package org.atlasapi.util;

import java.util.List;

import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.criteria.BooleanAttributeQuery;
import org.atlasapi.criteria.DateTimeAttributeQuery;
import org.atlasapi.criteria.EnumAttributeQuery;
import org.atlasapi.criteria.FloatAttributeQuery;
import org.atlasapi.criteria.IdAttributeQuery;
import org.atlasapi.criteria.IntegerAttributeQuery;
import org.atlasapi.criteria.MatchesNothing;
import org.atlasapi.criteria.QueryVisitor;
import org.atlasapi.criteria.SortAttributeQuery;
import org.atlasapi.criteria.StringAttributeQuery;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

/*
 * This is a dirty hack to allow SortBuilders to be extracted from a QueryAttributeSet while ignoring
 * non-sorting (i.e. not Ascending or Descending) operators.
 * TODO Refactor ES query building to not need this.
 */
public class EsSortQueryBuilder {

    public List<SortBuilder> buildSortQuery(AttributeQuerySet querySet) {
        return querySet.accept(new QueryVisitor<SortBuilder>() {

            @Override public SortBuilder visit(IntegerAttributeQuery query) {
                return null;
            }

            @Override public SortBuilder visit(StringAttributeQuery query) {
                return null;
            }

            @Override public SortBuilder visit(BooleanAttributeQuery query) {
                return null;
            }

            @Override public SortBuilder visit(EnumAttributeQuery<?> query) {
                return null;
            }

            @Override public SortBuilder visit(DateTimeAttributeQuery dateTimeAttributeQuery) {
                return null;
            }

            @Override public SortBuilder visit(MatchesNothing noOp) {
                return null;
            }

            @Override public SortBuilder visit(IdAttributeQuery query) {
                return null;
            }

            @Override public SortBuilder visit(FloatAttributeQuery query) {
                return null;
            }

            @Override public SortBuilder visit(SortAttributeQuery query) {
                List<String> fieldAndSortOrder = Splitter.on(".").splitToList(Iterables.getOnlyElement(query.getValue()));
                return SortBuilders
                        .fieldSort(fieldAndSortOrder.get(0))
                        .order(SortOrder.valueOf(fieldAndSortOrder.get(1).toUpperCase()));
            }
        }).stream().filter(r -> r != null).collect(ImmutableCollectors.toList());
    }
}