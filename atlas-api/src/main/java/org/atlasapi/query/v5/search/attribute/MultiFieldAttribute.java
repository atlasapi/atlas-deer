package org.atlasapi.query.v5.search.attribute;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.atlasapi.query.common.coercers.AttributeCoercer;
import org.atlasapi.query.common.exceptions.InvalidAttributeValueException;

import com.metabroadcast.sherlock.client.search.helpers.OccurenceClause;
import com.metabroadcast.sherlock.client.search.parameter.BoolParameter;
import com.metabroadcast.sherlock.client.search.parameter.TermParameter;
import com.metabroadcast.sherlock.common.type.ChildTypeMapping;

public class MultiFieldAttribute<T, M extends ChildTypeMapping<T>> extends
        SherlockBoolAttribute<T, T, M> {

    @SafeVarargs
    public MultiFieldAttribute(
            SherlockParameter parameter,
            AttributeCoercer<T> coercer,
            M... mappings
    ) {
        super(parameter, coercer, mappings);
    }

    @Override
    public List<BoolParameter> coerce(List<String> values) throws InvalidAttributeValueException {
        return coercer.apply(values).stream()
                .filter(Objects::nonNull)
                .map(v -> createParameter(mappings, v))
                .collect(Collectors.toList());
    }

    @Override
    protected BoolParameter createParameter(M[] mappings, T value) {
        return new BoolParameter(
                Arrays.stream(mappings)
                        .map(m -> TermParameter.of(m, value))
                        .collect(Collectors.toList()),
                OccurenceClause.SHOULD
        );
    }
}
