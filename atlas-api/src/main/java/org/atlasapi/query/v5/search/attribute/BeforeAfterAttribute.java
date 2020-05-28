package org.atlasapi.query.v5.search.attribute;

import java.time.Instant;

import javax.annotation.Nonnull;

import org.atlasapi.query.common.coercers.BooleanCoercer;

import com.metabroadcast.sherlock.client.search.parameter.RangeParameter;
import com.metabroadcast.sherlock.client.search.parameter.SimpleParameter;
import com.metabroadcast.sherlock.client.search.parameter.TermParameter;
import com.metabroadcast.sherlock.common.type.BooleanMapping;
import com.metabroadcast.sherlock.common.type.InstantMapping;

public class BeforeAfterAttribute extends SherlockAttributeSingle<Boolean, Instant, InstantMapping> {

    public BeforeAfterAttribute(
            SherlockParameter parameter,
            InstantMapping mapping
    ) {
        super(parameter, mapping, BooleanCoercer.create());
    }

    @Override
    protected SimpleParameter<Instant> createParameter(
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
