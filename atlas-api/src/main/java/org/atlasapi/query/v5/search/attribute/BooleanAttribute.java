package org.atlasapi.query.v5.search.attribute;

import javax.annotation.Nonnull;

import org.atlasapi.query.common.coercers.AttributeCoercer;
import org.atlasapi.query.common.coercers.BooleanCoercer;

import com.metabroadcast.sherlock.client.search.parameter.SimpleParameter;
import com.metabroadcast.sherlock.client.search.parameter.TermParameter;
import com.metabroadcast.sherlock.common.type.BooleanMapping;
import com.metabroadcast.sherlock.common.type.ChildTypeMapping;

public class BooleanAttribute extends TermAttribute<Boolean, BooleanMapping> {

    public BooleanAttribute(
            SherlockParameter parameter,
            BooleanMapping mapping
    ) {
        super(parameter, mapping, BooleanCoercer.create());
    }

    @Override
    protected SimpleParameter<Boolean> createParameter(BooleanMapping mapping, @Nonnull Boolean value) {
        return TermParameter.of(mapping, value);
    }
}
