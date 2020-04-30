package org.atlasapi.query.v5.search.attribute;

import javax.annotation.Nonnull;

import org.atlasapi.query.v5.search.coercer.Range;
import org.atlasapi.query.v5.search.coercer.RangeCoercer;

import com.metabroadcast.sherlock.client.search.parameter.NamedParameter;
import com.metabroadcast.sherlock.client.search.parameter.RangeParameter;
import com.metabroadcast.sherlock.client.search.parameter.TermParameter;
import com.metabroadcast.sherlock.common.type.RangeTypeMapping;

public class RangeAttribute<T> extends SherlockAttribute<Range<T>, T, RangeTypeMapping<T>> {

    public RangeAttribute(
            String parameterName,
            RangeTypeMapping<T> mapping,
            RangeCoercer<T, Range<T>> coercer
    ) {
        super(parameterName, mapping, coercer);
    }

    @Override
    protected NamedParameter<T> createParameter(RangeTypeMapping<T> mapping, @Nonnull Range<T> value) {
        if (value.getFrom() == value.getTo()) {
            return TermParameter.of(mapping, value.getFrom());
        } else {
            return RangeParameter.of(mapping, value.getFrom(), value.getTo());
        }
    }
}
