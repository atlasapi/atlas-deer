package org.atlasapi.query.v5.search.attribute;

import org.atlasapi.query.common.coercers.BooleanCoercer;

import com.metabroadcast.sherlock.client.search.helpers.OccurenceClause;
import com.metabroadcast.sherlock.client.search.parameter.BoolParameter;
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

        RangeParameter<T> from = RangeParameter.to(mappings[0], valueToBeWithinRange);
        RangeParameter<T> to = RangeParameter.from(mappings[1], valueToBeWithinRange);


        BoolParameter boolParameter = new BoolParameter(
                ImmutableList.of(from, to),
                OccurenceClause.MUST);

        if (value) {
            return boolParameter;
        } else {
            return (BoolParameter) boolParameter.negate();
        }
    }
}
