package org.atlasapi.query.v4.search.attribute;

import com.metabroadcast.sherlock.client.parameter.BoolParameter;
import com.metabroadcast.sherlock.client.parameter.ExistParameter;
import com.metabroadcast.sherlock.client.parameter.RangeParameter;
import com.metabroadcast.sherlock.client.parameter.SingleClauseBoolParameter;
import com.metabroadcast.sherlock.common.type.RangeTypeMapping;
import org.atlasapi.query.common.coercers.BooleanCoercer;

import java.util.function.Supplier;

public class BetweenRangeAttribute<T> extends SherlockBoolAttribute<Boolean, T, RangeTypeMapping<T>> {

    private final Supplier<T> valueToBeWithinRangeSupplier;

    public BetweenRangeAttribute(
            SherlockParameter parameter,
            RangeTypeMapping<T> lowerBoundMapping,
            RangeTypeMapping<T> upperBoundMapping,
            Supplier<T> valueToBeWithinRangeSupplier
    ) {
        super(parameter, BooleanCoercer.create(), lowerBoundMapping, upperBoundMapping);
        this.valueToBeWithinRangeSupplier = valueToBeWithinRangeSupplier;
    }

    public BetweenRangeAttribute(
            SherlockParameter parameter,
            RangeTypeMapping<T> mappingTo,
            RangeTypeMapping<T> mappingFrom,
            T valueToBeWithinRange
    ) {
        this(
                parameter,
                mappingTo,
                mappingFrom,
                () -> valueToBeWithinRange
        );
    }

    @Override
    protected BoolParameter createParameter(RangeTypeMapping<T>[] mappings, Boolean value) {
        final T valueToBeWithinRange = valueToBeWithinRangeSupplier.get();

        RangeTypeMapping<T> lowerBoundMapping = mappings[0];
        RangeTypeMapping<T> upperBoundMapping = mappings[1];

        RangeParameter<T> lowerBound = RangeParameter.to(lowerBoundMapping, valueToBeWithinRange);
        RangeParameter<T> upperBound = RangeParameter.from(upperBoundMapping, valueToBeWithinRange);

        ExistParameter<T> nullLowerBound = ExistParameter.notExists(lowerBoundMapping);
        SingleClauseBoolParameter nullLowerWithUpper = SingleClauseBoolParameter.must(nullLowerBound, upperBound);

        ExistParameter<T> nullUpperBound = ExistParameter.notExists(upperBoundMapping);
        SingleClauseBoolParameter lowerWithNullUpper = SingleClauseBoolParameter.must(lowerBound, nullUpperBound);

        SingleClauseBoolParameter both = SingleClauseBoolParameter.must(lowerBound, upperBound);
        SingleClauseBoolParameter neither = SingleClauseBoolParameter.must(nullLowerBound, nullUpperBound);

        SingleClauseBoolParameter inRange = SingleClauseBoolParameter.should(
                both,
                nullLowerWithUpper,
                lowerWithNullUpper,
                neither
        );

        if (value) {
            return inRange;
        } else {
            return inRange.negate();
        }
    }
}
