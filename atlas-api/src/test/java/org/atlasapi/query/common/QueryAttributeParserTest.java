package org.atlasapi.query.common;

import org.atlasapi.criteria.attribute.ContentAttributes;
import org.atlasapi.query.common.attributes.QueryAtomParser;
import org.atlasapi.query.common.attributes.QueryAttributeParser;
import org.atlasapi.query.common.coercers.IdCoercer;
import org.atlasapi.query.common.coercers.StringCoercer;
import org.atlasapi.query.common.exceptions.InvalidOperatorException;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.servlet.StubHttpServletRequest;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

public class QueryAttributeParserTest {

    private NumberToShortStringCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private final QueryAttributeParser parser = QueryAttributeParser.create(ImmutableList.of(
            QueryAtomParser.create(
                    ContentAttributes.ID,
                    IdCoercer.create(idCodec)
            ),
            QueryAtomParser.create(ContentAttributes.ALIASES_NAMESPACE, StringCoercer.create()),
            QueryAtomParser.create(ContentAttributes.ALIASES_VALUE, StringCoercer.create())
    ));

    @Test(expected = InvalidOperatorException.class)
    public void testThrowsExceptionForUnknownOperator() throws Exception {

        parser.parse(request()
                .withParam("aliases.namespace.begginning", "theNamespace")
        );

    }

    private StubHttpServletRequest request() {
        return new StubHttpServletRequest();
    }
}
