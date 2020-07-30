package org.atlasapi.query.v5.search.attribute;

import org.atlasapi.query.common.coercers.BooleanCoercer;

import com.metabroadcast.sherlock.client.helpers.OccurrenceClause;
import com.metabroadcast.sherlock.client.parameter.BoolParameter;
import com.metabroadcast.sherlock.client.parameter.ExistParameter;
import com.metabroadcast.sherlock.client.parameter.RangeParameter;
import com.metabroadcast.sherlock.client.parameter.SingleClauseBoolParameter;
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

        SingleClauseBoolParameter both = new SingleClauseBoolParameter(
                ImmutableList.of(from, to),
                OccurrenceClause.MUST);

        ExistParameter<T> nullFrom = ExistParameter.notExists(fromMapping);
        SingleClauseBoolParameter toAndNullFrom = new SingleClauseBoolParameter(
                ImmutableList.of(nullFrom, to),
                OccurrenceClause.MUST);

        ExistParameter<T> nullTo = ExistParameter.notExists(toMapping);
        SingleClauseBoolParameter fromAndNullTo = new SingleClauseBoolParameter(
                ImmutableList.of(nullTo, to),
                OccurrenceClause.MUST);

        SingleClauseBoolParameter inRange = new SingleClauseBoolParameter(
                ImmutableList.of(both, toAndNullFrom, fromAndNullTo),
                OccurrenceClause.SHOULD
        );

        if (value) {
            return inRange;
        } else {
            return inRange.negate();
        }
    }
}
