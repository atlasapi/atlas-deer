package org.atlasapi.query.v5.search.attribute;

import java.util.List;
import java.util.stream.Collectors;

import org.atlasapi.query.common.coercers.AttributeCoercer;
import org.atlasapi.query.common.coercers.EnumCoercer;
import org.atlasapi.query.common.exceptions.InvalidAttributeValueException;

import com.metabroadcast.sherlock.client.search.parameter.NamedParameter;
import com.metabroadcast.sherlock.client.search.parameter.TermParameter;
import com.metabroadcast.sherlock.common.type.ChildTypeMapping;

public class EnumAttribute<E extends Enum<E>> extends SherlockAttribute<String, ChildTypeMapping<String>> {

    private final EnumCoercer<E> coercer;

    public EnumAttribute(
            String parameterName,
            ChildTypeMapping<String> mapping,
            EnumCoercer<E> coercer
    ) {
        super(parameterName, mapping);
        this.coercer = coercer;
    }

    @Override
    public List<NamedParameter<String>> coerce(List<String> value) throws InvalidAttributeValueException {
        return coercer.apply(value).stream()
                .map(Enum::toString)
                .map(v -> TermParameter.of(getMapping(), v))
                .collect(Collectors.toList());
    }
}
