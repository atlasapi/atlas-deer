package org.atlasapi.query.v4.search.attribute;

import com.metabroadcast.sherlock.client.parameter.RangeParameter;
import com.metabroadcast.sherlock.client.parameter.SingleValueParameter;
import com.metabroadcast.sherlock.client.parameter.TermParameter;
import com.metabroadcast.sherlock.common.type.RangeTypeMapping;
import org.atlasapi.query.v4.search.coercer.Range;
import org.atlasapi.query.v4.search.coercer.RangeCoercer;

import javax.annotation.Nonnull;

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
    protected SingleValueParameter<T> createParameter(RangeTypeMapping<T> mapping, @Nonnull Range<T> value) {
        return getRangeOrTerm(mapping, value);
    }

    public static <T> SingleValueParameter<T> getRangeOrTerm(RangeTypeMapping<T> mapping, @Nonnull Range<T> value) {
        if (value.getFrom() == value.getTo()) {
            return TermParameter.of(mapping, value.getFrom());
        } else {
            return RangeParameter.of(mapping, value.getFrom(), value.getTo());
        }
    }
}
