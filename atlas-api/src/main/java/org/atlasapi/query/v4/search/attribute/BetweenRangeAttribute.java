package org.atlasapi.query.v4.search.attribute;

import com.metabroadcast.sherlock.client.parameter.BoolParameter;
import com.metabroadcast.sherlock.client.parameter.ExistParameter;
import com.metabroadcast.sherlock.client.parameter.RangeParameter;
import com.metabroadcast.sherlock.client.parameter.SingleClauseBoolParameter;
import com.metabroadcast.sherlock.common.type.RangeTypeMapping;
import org.atlasapi.query.common.coercers.BooleanCoercer;

public class BetweenRangeAttribute<T> extends SherlockBoolAttribute<Boolean, T, RangeTypeMapping<T>> {

    private final T valueToBeWithinRange;

    public BetweenRangeAttribute(
            SherlockParameter parameter,
            RangeTypeMapping<T> mappingTo,
            RangeTypeMapping<T> mappingFrom,
            T valueToBeWithinRange
    ) {
        super(parameter, BooleanCoercer.create(), mappingTo, mappingFrom);
        this.valueToBeWithinRange = valueToBeWithinRange;
    }

    @Override
    protected BoolParameter createParameter(RangeTypeMapping<T>[] mappings, Boolean value) {

        RangeTypeMapping<T> toMapping = mappings[0];
        RangeTypeMapping<T> fromMapping = mappings[1];

        RangeParameter<T> from = RangeParameter.to(toMapping, valueToBeWithinRange);
        RangeParameter<T> to = RangeParameter.from(fromMapping, valueToBeWithinRange);

        SingleClauseBoolParameter both = SingleClauseBoolParameter.must(from, to);

        ExistParameter<T> nullFrom = ExistParameter.notExists(fromMapping);
        SingleClauseBoolParameter toAndNullFrom = SingleClauseBoolParameter.must(nullFrom, to);

        ExistParameter<T> nullTo = ExistParameter.notExists(toMapping);
        SingleClauseBoolParameter fromAndNullTo = SingleClauseBoolParameter.must(nullTo, to);

        SingleClauseBoolParameter inRange = SingleClauseBoolParameter.should(
                both,
                toAndNullFrom,
                fromAndNullTo
        );

        if (value) {
            return inRange;
        } else {
            return inRange.negate();
        }
    }
}
