package org.atlasapi.query.v5.search.attribute;

import java.util.Arrays;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.atlasapi.query.common.coercers.AttributeCoercer;

import com.metabroadcast.sherlock.client.search.helpers.OccurenceClause;
import com.metabroadcast.sherlock.client.search.parameter.BoolParameter;
import com.metabroadcast.sherlock.client.search.parameter.SimpleParameter;
import com.metabroadcast.sherlock.common.type.ChildTypeMapping;

public class MultiFieldAttribute<FROM, TO, M extends ChildTypeMapping<TO>> extends
        SherlockBoolAttribute<FROM, TO, M> {

    private final BiFunction<M, FROM, SimpleParameter<TO>> parameterBiFunction;

    @SafeVarargs
    public MultiFieldAttribute(
            SherlockParameter parameter,
            AttributeCoercer<FROM> coercer,
            BiFunction<M, FROM, SimpleParameter<TO>> parameterBiFunction,
            M... mappings
    ) {
        super(parameter, coercer, mappings);
        this.parameterBiFunction = parameterBiFunction;
    }

    @Override
    protected BoolParameter createParameter(M[] mappings, FROM value) {
        return new BoolParameter(
                Arrays.stream(mappings)
                        .map(m -> parameterBiFunction.apply(m, value))
                        .collect(Collectors.toList()),
                OccurenceClause.SHOULD
        );
    }
}
