package org.atlasapi.query.common;

import javax.servlet.http.HttpServletRequest;

import com.metabroadcast.applications.client.model.internal.Application;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.attribute.Attributes;
import org.atlasapi.entity.Id;
import org.atlasapi.query.annotation.ActiveAnnotations;
import org.atlasapi.query.common.attributes.QueryAtomParser;
import org.atlasapi.query.common.attributes.QueryAttributeParser;
import org.atlasapi.query.common.coercers.IdCoercer;
import org.atlasapi.query.common.coercers.StringCoercer;
import org.atlasapi.query.common.context.QueryContext;
import org.atlasapi.query.common.context.QueryContextParser;
import org.atlasapi.query.common.exceptions.InvalidOperatorException;
import org.atlasapi.query.common.exceptions.InvalidParameterException;
import org.atlasapi.topic.Topic;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.servlet.StubHttpServletRequest;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class StandardQueryParserTest {

    private final NumberToShortStringCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private final QueryAttributeParser atrributes = QueryAttributeParser.create(
            ImmutableList.of(
                    QueryAtomParser.create(
                            Attributes.ID,
                            IdCoercer.create(idCodec)
                    ),
                    QueryAtomParser.create(Attributes.ALIASES_NAMESPACE, StringCoercer.create())
            )
    );

    @Mock private QueryContextParser queryContextParser;
    @Mock private HttpServletRequest request = mock(HttpServletRequest.class);
    @Mock private Application application = mock(Application.class);

    private QueryContext queryContext = QueryContext.create(
            application,
            ActiveAnnotations.standard(),
            request
    );
    private StandardQueryParser<Topic> queryParser;

    @Before
    public void setUp() {
        when(queryContextParser.getRequiredParameters()).thenReturn(ImmutableSet.of());
        when(queryContextParser.getOptionalParameters()).thenReturn(ImmutableSet.of());
        queryParser = StandardQueryParser.create(
                Resource.TOPIC,
                atrributes,
                idCodec,
                queryContextParser
        );
    }

    @Test
    public void testParsesSingleIdIntoNonListTopicQuery() throws Exception {
        when(queryContextParser.parseSingleContext(isA(HttpServletRequest.class)))
                .thenReturn(queryContext);

        Query<Topic> q = queryParser.parse(requestWithPath("4.0/topics/cbbh.json"));

        assertFalse(q.isListQuery());
        assertThat(q.getOnlyId(), is(Id.valueOf(idCodec.decode("cbbh"))));

        verify(queryContextParser).parseSingleContext(isA(HttpServletRequest.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testParsesIdsOnlyIntoListQuery() throws Exception {
        when(queryContextParser.parseListContext(isA(HttpServletRequest.class)))
                .thenReturn(queryContext);

        Query<Topic> q = queryParser.parse(requestWithPath("4.0/topics.json")
                .withParam("id", "cbbh"));

        assertTrue(q.isListQuery());
        assertThat(Iterables.size(q.getOperands()), is(1));
        AttributeQuery<Id> operand = (AttributeQuery<Id>) Iterables.getOnlyElement(q.getOperands());
        assertThat(operand.getValue(), hasItem(Id.valueOf(idCodec.decode("cbbh"))));

        verify(queryContextParser).parseListContext(isA(HttpServletRequest.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testParsesAttributeKeyWithOperator() throws Exception {
        when(queryContextParser.parseListContext(isA(HttpServletRequest.class)))
                .thenReturn(queryContext);

        Query<Topic> q = queryParser.parse(requestWithPath("4.0/topics.json")
                .withParam("aliases.namespace.beginning", "prefix"));

        assertTrue(q.isListQuery());
        assertThat(Iterables.size(q.getOperands()), is(1));
        AttributeQuery<String> operand =
                (AttributeQuery<String>) Iterables.getOnlyElement(q.getOperands());
        assertThat(operand.getValue(), hasItem("prefix"));

        verify(queryContextParser).parseListContext(isA(HttpServletRequest.class));
    }

    @Test(expected = InvalidParameterException.class)
    public void testRejectsInvalidAttributeKey() throws Exception {
        when(queryContextParser.parseListContext(isA(HttpServletRequest.class)))
                .thenReturn(queryContext);

        queryParser.parse(requestWithPath("4.0/topics.json")
                .withParam("just.the.beginning", "prefix"));

    }

    @Test(expected = InvalidOperatorException.class)
    public void testRejectsAttributeKeyWithBadOperator() throws Exception {
        when(queryContextParser.parseListContext(isA(HttpServletRequest.class)))
                .thenReturn(queryContext);

        queryParser.parse(requestWithPath("4.0/topics.json")
                .withParam("aliases.namespace.ending", "suffix"));

    }

    private StubHttpServletRequest requestWithPath(String uri) {
        return new StubHttpServletRequest().withRequestUri(uri);
    }

}
