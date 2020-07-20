package org.atlasapi.query.v5.search.attribute;

import java.time.Instant;

import javax.annotation.Nonnull;

import org.atlasapi.query.common.coercers.BooleanCoercer;

import com.metabroadcast.sherlock.client.search.parameter.RangeParameter;
import com.metabroadcast.sherlock.client.search.parameter. SingleValueParameter;
import com.metabroadcast.sherlock.common.type.InstantMapping;

public class BeforeAfterAttribute extends
        SherlockSingleMappingAttribute<Boolean, Instant, InstantMapping> {

    public BeforeAfterAttribute(
            SherlockParameter parameter,
            InstantMapping mapping
    ) {
        super(parameter, BooleanCoercer.create(), mapping);
    }

    @Override
    protected  SingleValueParameter<Instant> createParameter(
            InstantMapping mapping,
            @Nonnull Boolean value
    ) {
        if (value) {
            return RangeParameter.from(mapping, Instant.now());
        } else {
            return RangeParameter.to(mapping, Instant.now());
        }
    }
}
