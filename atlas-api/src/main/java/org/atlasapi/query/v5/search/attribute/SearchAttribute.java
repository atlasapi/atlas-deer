package org.atlasapi.query.v5.search.attribute;

import javax.annotation.Nonnull;

import org.atlasapi.query.common.coercers.AttributeCoercer;
import org.atlasapi.query.common.coercers.StringCoercer;

import com.metabroadcast.sherlock.client.search.parameter.NamedParameter;
import com.metabroadcast.sherlock.client.search.parameter.SearchParameter;
import com.metabroadcast.sherlock.client.search.parameter.TermParameter;
import com.metabroadcast.sherlock.common.type.ChildTypeMapping;
import com.metabroadcast.sherlock.common.type.TextMapping;

public class SearchAttribute extends SherlockAttribute<String, String, TextMapping> {

    public SearchAttribute(
            SherlockParameter parameter,
            TextMapping mapping
    ) {
        super(parameter, mapping, StringCoercer.create());
    }

    @Override
    protected NamedParameter<String> createParameter(TextMapping mapping, @Nonnull String value) {
        return SearchParameter.builder()
                .withMapping(mapping)
                .withValue(value)
                .build();
    }
}
