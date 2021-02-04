package org.atlasapi.query.v4.search.attribute;

import com.metabroadcast.sherlock.client.parameter.RangeParameter;
import com.metabroadcast.sherlock.client.parameter.SingleValueParameter;
import com.metabroadcast.sherlock.common.type.InstantMapping;
import org.atlasapi.query.common.coercers.BooleanCoercer;

import javax.annotation.Nonnull;
import java.time.Instant;

public class BeforeAfterAttribute extends
        SherlockSingleMappingAttribute<Boolean, Instant, InstantMapping> {

    public BeforeAfterAttribute(
            SherlockParameter parameter,
            InstantMapping mapping
    ) {
        super(parameter, BooleanCoercer.create(), mapping);
    }

    @Override
    protected SingleValueParameter<Instant> createParameter(
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
