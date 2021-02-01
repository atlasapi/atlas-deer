package org.atlasapi.query.v4.search.attribute;

import com.metabroadcast.sherlock.client.parameter.SingleValueParameter;
import com.metabroadcast.sherlock.client.parameter.TermParameter;
import com.metabroadcast.sherlock.common.type.BooleanMapping;
import org.atlasapi.query.common.coercers.BooleanCoercer;

import javax.annotation.Nonnull;

public class BooleanAttribute extends TermAttribute<Boolean, BooleanMapping> {

    public BooleanAttribute(
            SherlockParameter parameter,
            BooleanMapping mapping
    ) {
        super(parameter, mapping, BooleanCoercer.create());
    }

    @Override
    protected SingleValueParameter<Boolean> createParameter(BooleanMapping mapping, @Nonnull Boolean value) {
        return TermParameter.of(mapping, value);
    }
}
