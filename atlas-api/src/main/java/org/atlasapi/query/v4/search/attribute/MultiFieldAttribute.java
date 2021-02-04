package org.atlasapi.query.v4.search.attribute;

import com.metabroadcast.sherlock.client.parameter.SingleClauseBoolParameter;
import com.metabroadcast.sherlock.client.parameter.SingleValueParameter;
import com.metabroadcast.sherlock.common.type.ChildTypeMapping;
import org.atlasapi.query.common.coercers.AttributeCoercer;

import java.util.Arrays;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class MultiFieldAttribute<FROM, TO, M extends ChildTypeMapping<TO>> extends
        SherlockBoolAttribute<FROM, TO, M> {

    private final BiFunction<M, FROM, SingleValueParameter<TO>> parameterBiFunction;

    @SafeVarargs
    public MultiFieldAttribute(
            SherlockParameter parameter,
            AttributeCoercer<FROM> coercer,
            BiFunction<M, FROM, SingleValueParameter<TO>> parameterBiFunction,
            M... mappings
    ) {
        super(parameter, coercer, mappings);
        this.parameterBiFunction = parameterBiFunction;
    }

    @Override
    protected SingleClauseBoolParameter createParameter(M[] mappings, FROM value) {
        return SingleClauseBoolParameter.should(
                Arrays.stream(mappings)
                        .map(m -> parameterBiFunction.apply(m, value))
                        .collect(Collectors.toList())
        );
    }
}
