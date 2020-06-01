package org.atlasapi.query.v5.search.attribute;

import org.atlasapi.query.common.coercers.BooleanCoercer;

import com.metabroadcast.sherlock.client.search.helpers.OccurenceClause;
import com.metabroadcast.sherlock.client.search.parameter.BoolParameter;
import com.metabroadcast.sherlock.client.search.parameter.ExistParameter;
import com.metabroadcast.sherlock.client.search.parameter.RangeParameter;
import com.metabroadcast.sherlock.common.type.RangeTypeMapping;

import com.google.common.collect.ImmutableList;

public class BetweenRangeAttribute<T> extends SherlockBoolAttribute<Boolean, T, RangeTypeMapping<T>> {

    private final T valueToBeWithinRange;

    public BetweenRangeAttribute(
            SherlockParameter parameter,
            RangeTypeMapping<T> mappingFrom,
            RangeTypeMapping<T> mappingTo,
            T valueToBeWithinRange
    ) {
        super(parameter, BooleanCoercer.create(), mappingFrom, mappingTo);
        this.valueToBeWithinRange = valueToBeWithinRange;
    }

    @Override
    protected BoolParameter createParameter(RangeTypeMapping<T>[] mappings, Boolean value) {

        RangeTypeMapping<T> fromMapping = mappings[0];
        RangeTypeMapping<T> toMapping = mappings[1];

        RangeParameter<T> from = RangeParameter.to(fromMapping, valueToBeWithinRange);
        RangeParameter<T> to = RangeParameter.from(toMapping, valueToBeWithinRange);

        BoolParameter both = new BoolParameter(
                ImmutableList.of(from, to),
                OccurenceClause.MUST);

        ExistParameter<T> nullFrom = ExistParameter.notExists(fromMapping);
        BoolParameter toAndNullFrom = new BoolParameter(
                ImmutableList.of(nullFrom, to),
                OccurenceClause.MUST);

        ExistParameter<T> nullTo = ExistParameter.notExists(toMapping);
        BoolParameter fromAndNullTo = new BoolParameter(
                ImmutableList.of(nullTo, to),
                OccurenceClause.MUST);

        BoolParameter inRange = new BoolParameter(
                ImmutableList.of(both, toAndNullFrom, fromAndNullTo),
                OccurenceClause.SHOULD
        );

        if (value) {
            return inRange;
        } else {
            return  (BoolParameter) inRange.negate();
        }
    }
}
