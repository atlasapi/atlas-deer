package org.atlasapi.query.v5.search.attribute;

import javax.annotation.Nonnull;

import org.atlasapi.query.v5.search.coercer.Range;
import org.atlasapi.query.v5.search.coercer.RangeCoercer;

import com.metabroadcast.sherlock.client.search.parameter.RangeParameter;
import com.metabroadcast.sherlock.client.search.parameter.SimpleParameter;
import com.metabroadcast.sherlock.client.search.parameter.TermParameter;
import com.metabroadcast.sherlock.common.type.RangeTypeMapping;

public class RangeAttribute<T> extends
        SherlockSingleMappingAttribute<Range<T>, T, RangeTypeMapping<T>> {

    public RangeAttribute(
            SherlockParameter parameter,
            RangeTypeMapping<T> mapping,
            RangeCoercer<T> coercer
    ) {
        super(parameter, coercer, mapping);
    }

    @Override
    protected SimpleParameter<T> createParameter(RangeTypeMapping<T> mapping, @Nonnull Range<T> value) {
        return getRangeOrTerm(mapping, value);
    }

    public static <T> SimpleParameter<T> getRangeOrTerm(RangeTypeMapping<T> mapping, @Nonnull Range<T> value) {
        if (value.getFrom() == value.getTo()) {
            return TermParameter.of(mapping, value.getFrom());
        } else {
            return RangeParameter.of(mapping, value.getFrom(), value.getTo());
        }
    }
}
