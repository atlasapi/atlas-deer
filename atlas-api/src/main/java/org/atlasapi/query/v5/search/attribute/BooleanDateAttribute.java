package org.atlasapi.query.v5.search.attribute;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import org.atlasapi.query.common.coercers.BooleanCoercer;
import org.atlasapi.query.common.exceptions.InvalidAttributeValueException;

import com.metabroadcast.sherlock.client.search.parameter.NamedParameter;
import com.metabroadcast.sherlock.client.search.parameter.RangeParameter;
import com.metabroadcast.sherlock.common.type.DateMapping;

public class BooleanDateAttribute extends SherlockAttribute<Instant, DateMapping> {

    private final BooleanCoercer coercer;

    public BooleanDateAttribute(
            String parameterName,
            DateMapping mapping
    ) {
        super(parameterName, mapping);
        coercer = BooleanCoercer.create();
    }

    @Override
    public List<NamedParameter<Instant>> coerce(List<String> values) throws InvalidAttributeValueException {
        return coercer.apply(values).stream()
                .map(v -> coerceToScheduleRange(getMapping(), v))
                .collect(Collectors.toList());
    }

    private RangeParameter<Instant> coerceToScheduleRange(
            DateMapping mapping,
            Boolean upcoming
    ) {
        if (upcoming) {
            return RangeParameter.from(mapping, Instant.now());
        } else {
            return RangeParameter.to(mapping, Instant.now());
        }
    }
    
}
