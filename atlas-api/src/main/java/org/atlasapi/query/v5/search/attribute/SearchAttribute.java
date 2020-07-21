package org.atlasapi.query.v5.search.attribute;

import javax.annotation.Nonnull;

import org.atlasapi.query.common.coercers.StringCoercer;

import com.metabroadcast.sherlock.client.search.parameter.SearchParameter;
import com.metabroadcast.sherlock.client.search.parameter.SingleValueParameter;
import com.metabroadcast.sherlock.common.type.TextMapping;

public class SearchAttribute extends
        SherlockSingleMappingAttribute<String, String, TextMapping> {

    public SearchAttribute(
            SherlockParameter parameter,
            TextMapping mapping
    ) {
        super(parameter, StringCoercer.create(), mapping);
    }

    @Override
    protected SingleValueParameter<String> createParameter(TextMapping mapping, @Nonnull String value) {
        return SearchParameter.of(mapping, value);
    }
}
