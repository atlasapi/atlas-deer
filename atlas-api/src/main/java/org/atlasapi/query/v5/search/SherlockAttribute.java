package org.atlasapi.query.v5.search;

import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.atlasapi.query.common.coercers.AttributeCoercer;
import org.atlasapi.query.common.exceptions.InvalidAttributeValueException;

import com.metabroadcast.sherlock.client.search.parameter.NamedParameter;
import com.metabroadcast.sherlock.client.search.parameter.TermParameter;
import com.metabroadcast.sherlock.common.type.ChildTypeMapping;

public abstract class SherlockAttribute<T, M extends ChildTypeMapping<T>> {

    private final String parameterName;
    private final M mapping;

    public SherlockAttribute(
            String parameterName,
            M mapping
    ) {
        this.parameterName = parameterName;
        this.mapping = mapping;
    }

    public String getParameterName() {
        return parameterName;
    }

    public M getMapping() {
        return mapping;
    }

    public abstract List<NamedParameter<T>> coerce(List<String> value)
            throws InvalidAttributeValueException;
}
