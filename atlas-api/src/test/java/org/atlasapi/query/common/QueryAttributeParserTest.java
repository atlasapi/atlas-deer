package org.atlasapi.query.common;

import java.util.List;

import org.atlasapi.criteria.AtomicQuery;
import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.criteria.IdAttributeQuery;
import org.atlasapi.criteria.QueryVisitorAdapter;
import org.atlasapi.criteria.StringAttributeQuery;
import org.atlasapi.criteria.attribute.Attributes;
import org.atlasapi.criteria.operator.Operators;
import org.atlasapi.entity.Id;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.servlet.StubHttpServletRequest;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class QueryAttributeParserTest {

    private NumberToShortStringCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private final QueryAttributeParser parser = new QueryAttributeParser(ImmutableList.of(
            QueryAtomParser.valueOf(Attributes.ID, AttributeCoercers.idCoercer(idCodec)),
            QueryAtomParser.valueOf(
                    Attributes.ALIASES_NAMESPACE,
                    AttributeCoercers.stringCoercer()
            ),
            QueryAtomParser.valueOf(Attributes.ALIASES_VALUE, AttributeCoercers.stringCoercer())
    ));

    @Test
    public void testParseQueryWithIdAttribute() throws Exception {
        final Id id1 = Id.valueOf(1234);
        final Id id2 = Id.valueOf(1235);

        String idParam = String.format("%s,%s",
                idCodec.encode(id1.toBigInteger()), idCodec.encode(id2.toBigInteger())
        );

        AttributeQuerySet queries = parser.parse(request().withParam("id", idParam));

        AtomicQuery idAttributeQuery = Iterables.getOnlyElement(queries);

        assertTrue(idAttributeQuery.accept(new QueryVisitorAdapter<Boolean>() {

            @Override
            public Boolean visit(IdAttributeQuery query) {
                return query.getValue().containsAll(ImmutableList.of(id1, id2));
            }

            @Override
            protected Boolean defaultValue(AtomicQuery query) {
                return false;
            }
        }));
    }

    @Test
    public void testParseQueryWithMultipleParams() throws Exception {

        AttributeQuerySet queries = parser.parse(request()
                .withParam("aliases.namespace", "theNamespace")
                .withParam("aliases.value", "theValue")
        );

        assertThat(queries.size(), is(2));
        List<Boolean> accept = queries.accept(new QueryVisitorAdapter<Boolean>() {

            @Override
            public Boolean visit(StringAttributeQuery query) {
                if (query.getAttributeName().equals(Attributes.ALIASES_NAMESPACE.externalName())) {
                    return Iterables.getOnlyElement(query.getValue()).equals("theNamespace");
                }
                if (query.getAttributeName().equals(Attributes.ALIASES_VALUE.externalName())) {
                    return Iterables.getOnlyElement(query.getValue()).equals("theValue");
                }
                return false;
            }

            @Override
            protected Boolean defaultValue(AtomicQuery query) {
                return false;
            }
        });

        assertTrue(Iterables.all(accept, Predicates.equalTo(true)));

    }

    @Test
    public void testParseQueryWithOperator() throws Exception {

        AttributeQuerySet queries = parser.parse(request()
                .withParam("aliases.namespace.beginning", "theNamespace")
                .withParam("aliases.value.equals", "theValue")
        );

        assertThat(queries.size(), is(2));
        List<Boolean> accept = queries.accept(new QueryVisitorAdapter<Boolean>() {

            @Override
            public Boolean visit(StringAttributeQuery query) {
                if (query.getAttributeName().equals(Attributes.ALIASES_NAMESPACE.externalName())) {
                    return Iterables.getOnlyElement(query.getValue()).equals("theNamespace")
                            && query.getOperator().equals(Operators.BEGINNING);
                }
                if (query.getAttributeName().equals(Attributes.ALIASES_VALUE.externalName())) {
                    return Iterables.getOnlyElement(query.getValue()).equals("theValue")
                            && query.getOperator().equals(Operators.EQUALS);
                }
                return false;
            }

            @Override
            protected Boolean defaultValue(AtomicQuery query) {
                return false;
            }
        });

        assertTrue(Iterables.all(accept, Predicates.equalTo(true)));

    }

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
