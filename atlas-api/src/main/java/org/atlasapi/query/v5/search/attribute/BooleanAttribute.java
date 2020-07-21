package org.atlasapi.query.v5.search.attribute;

import javax.annotation.Nonnull;

import org.atlasapi.query.common.coercers.BooleanCoercer;

import com.metabroadcast.sherlock.client.search.parameter.SingleValueParameter;
import com.metabroadcast.sherlock.client.search.parameter.TermParameter;
import com.metabroadcast.sherlock.common.type.BooleanMapping;

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
