package org.atlasapi.query.v5.search.attribute;

import java.util.List;
import java.util.stream.Collectors;

import org.atlasapi.query.common.coercers.AttributeCoercer;
import org.atlasapi.query.common.coercers.EnumCoercer;
import org.atlasapi.query.common.exceptions.InvalidAttributeValueException;

import com.metabroadcast.sherlock.client.search.parameter.NamedParameter;
import com.metabroadcast.sherlock.client.search.parameter.TermParameter;
import com.metabroadcast.sherlock.common.type.ChildTypeMapping;
import com.metabroadcast.sherlock.common.type.KeywordMapping;

public class EnumAttribute extends SherlockAttribute<String, KeywordMapping> {

    private final EnumCoercer<?> coercer;

    public EnumAttribute(
            String parameterName,
            KeywordMapping mapping,
            EnumCoercer<?> coercer
    ) {
        super(parameterName, mapping);
        this.coercer = coercer;
    }

    @Override
    public List<NamedParameter<String>> coerce(List<String> values) throws InvalidAttributeValueException {
        return coercer.apply(values).stream()
                .map(Enum::toString)
                .map(v -> TermParameter.of(getMapping(), v))
                .collect(Collectors.toList());
    }
}
