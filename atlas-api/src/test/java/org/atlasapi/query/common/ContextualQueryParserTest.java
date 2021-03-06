package org.atlasapi.query.common;

import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.content.Content;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.attribute.Attributes;
import org.atlasapi.entity.Id;
import org.atlasapi.query.annotation.ActiveAnnotations;
import org.atlasapi.query.common.attributes.QueryAttributeParser;
import org.atlasapi.query.common.context.QueryContext;
import org.atlasapi.topic.Topic;

import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.servlet.StubHttpServletRequest;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ContextualQueryParserTest {

    @Mock private QueryAttributeParser attributeParser;
    @Mock private ContextualQueryContextParser queryContextParser;

    private final NumberToShortStringCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private ContextualQueryParser<Topic, Content> parser;

    @Before
    public void setUp() {
        when(queryContextParser.getOptionalParameters()).thenReturn(ImmutableSet.<String>of());
        when(queryContextParser.getRequiredParameters()).thenReturn(ImmutableSet.<String>of());
        when(attributeParser.getOptionalParameters()).thenReturn(ImmutableSet.of("alias.namespace"));
        this.parser = new ContextualQueryParser<Topic, Content>(Resource.TOPIC, Attributes.TOPIC_ID,
                Resource.CONTENT, idCodec, attributeParser, queryContextParser
        );
    }

    @Before
    public void before() {
    }

    @Test
    public void testParseRequest() throws Exception {

        HttpServletRequest req = new StubHttpServletRequest().withRequestUri(
                "/4/topics/cbbh/content.json"
        ).withParam("alias.namespace", "ns");

        when(attributeParser.parse(req))
                .thenReturn(ImmutableSet.of());
        when(queryContextParser.parseContext(req))
                .thenReturn(QueryContext.create(
                        mock(Application.class),
                        ActiveAnnotations.standard(),
                        mock(HttpServletRequest.class)
                ));

        ContextualQuery<Topic, Content> query = parser.parse(req);

        Id contextId = query.getContextQuery().getOnlyId();
        assertThat(idCodec.encode(contextId.toBigInteger()), is("cbbh"));

        Set<AttributeQuery<?>> resourceQuerySet = query.getResourceQuery().getOperands();
        AttributeQuery<?> contextAttributeQuery = Iterables.getOnlyElement(resourceQuerySet);

        assertThat(contextAttributeQuery.getValue().get(0), is(Id.valueOf(idCodec.decode("cbbh"))));

        assertTrue(query.getContext() == query.getContextQuery().getContext());
        assertTrue(query.getContext() == query.getResourceQuery().getContext());

        verify(attributeParser, times(1)).parse(req);
        verify(queryContextParser, times(1)).parseContext(req);

    }

}
