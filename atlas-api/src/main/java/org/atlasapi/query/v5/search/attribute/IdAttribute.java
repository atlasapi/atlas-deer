package org.atlasapi.query.v5.search.attribute;

import java.util.List;
import java.util.stream.Collectors;

import org.atlasapi.query.common.coercers.EnumCoercer;
import org.atlasapi.query.common.coercers.IdCoercer;
import org.atlasapi.query.common.exceptions.InvalidAttributeValueException;

import com.metabroadcast.sherlock.client.search.parameter.NamedParameter;
import com.metabroadcast.sherlock.client.search.parameter.TermParameter;
import com.metabroadcast.sherlock.common.type.ChildTypeMapping;
import com.metabroadcast.sherlock.common.type.KeywordMapping;

public class IdAttribute extends SherlockAttribute<String, KeywordMapping> {

    private final IdCoercer coercer;

    public IdAttribute(
            String parameterName,
            KeywordMapping mapping,
            IdCoercer coercer
    ) {
        super(parameterName, mapping);
        this.coercer = coercer;
    }

    @Override
    public List<NamedParameter<String>> coerce(List<String> values) {
        coercer.apply(values); // throws exception if input are not ids
        return values.stream()
                .map(v -> TermParameter.of(getMapping(), v))
                .collect(Collectors.toList());
    }
}
