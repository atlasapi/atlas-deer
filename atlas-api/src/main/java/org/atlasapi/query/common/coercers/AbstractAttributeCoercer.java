package org.atlasapi.query.common.coercers;

import java.util.List;
import java.util.stream.StreamSupport;

import com.metabroadcast.common.stream.MoreCollectors;

public abstract class AbstractAttributeCoercer<O> implements AttributeCoercer<O> {

    @Override
    public final List<O> apply(Iterable<String> input) {
        return StreamSupport.stream(input.spliterator(), false)
                .map(this::coerce)
                .collect(MoreCollectors.toImmutableList());
    }

    protected abstract O coerce(String input);
}
