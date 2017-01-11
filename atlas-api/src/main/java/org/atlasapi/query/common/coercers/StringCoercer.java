package org.atlasapi.query.common.coercers;

import java.util.List;

import com.google.common.collect.ImmutableList;

public class StringCoercer implements AttributeCoercer<String> {

    private StringCoercer() {
    }

    public static StringCoercer create() {
        return new StringCoercer();
    }

    @Override
    public List<String> apply(Iterable<String> input) {
        return ImmutableList.copyOf(input);
    }
}
