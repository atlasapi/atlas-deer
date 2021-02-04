package org.atlasapi.query.v4.search.attribute;

import com.metabroadcast.sherlock.client.parameter.SearchParameter;
import com.metabroadcast.sherlock.client.parameter.SingleValueParameter;
import com.metabroadcast.sherlock.common.type.TextMapping;
import org.atlasapi.query.common.coercers.StringCoercer;

import javax.annotation.Nonnull;

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
