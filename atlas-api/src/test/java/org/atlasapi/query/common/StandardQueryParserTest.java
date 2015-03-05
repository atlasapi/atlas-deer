package org.atlasapi.query.common;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.attribute.Attributes;
import org.atlasapi.entity.Id;
import org.atlasapi.topic.Topic;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.servlet.StubHttpServletRequest;

@RunWith(MockitoJUnitRunner.class)
public class StandardQueryParserTest {

    private final NumberToShortStringCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private final QueryAttributeParser atrributes = new QueryAttributeParser(
        ImmutableList.of(
            QueryAtomParser.valueOf(Attributes.ID, AttributeCoercers.idCoercer(idCodec)), 
            QueryAtomParser.valueOf(Attributes.ALIASES_NAMESPACE, AttributeCoercers.stringCoercer())
        )
    );
    
    @Mock private QueryContextParser queryContextParser;
    private StandardQueryParser<Topic> queryParser; 
    
    @Before
    public void setUp() {
        when(queryContextParser.getRequiredParameters()).thenReturn(ImmutableSet.<String>of());
        when(queryContextParser.getOptionalParameters()).thenReturn(ImmutableSet.<String>of());
        queryParser = new StandardQueryParser<Topic>(Resource.TOPIC, atrributes, idCodec, queryContextParser);
    }
    
    @Test
    public void testParsesSingleIdIntoNonListTopicQuery() throws Exception {
        when(queryContextParser.parseSingleContext(isA(HttpServletRequest.class)))
            .thenReturn(QueryContext.standard(mock(HttpServletRequest.class)));
        
        Query<Topic> q = queryParser.parse(requestWithPath("4.0/topics/cbbh.json"));
        
        assertFalse(q.isListQuery());
        assertThat(q.getOnlyId(), is(Id.valueOf(idCodec.decode("cbbh"))));
        
        verify(queryContextParser).parseSingleContext(isA(HttpServletRequest.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testParsesIdsOnlyIntoListQuery() throws Exception {
        when(queryContextParser.parseListContext(isA(HttpServletRequest.class)))
            .thenReturn(QueryContext.standard(mock(HttpServletRequest.class)));
    
        Query<Topic> q = queryParser.parse(requestWithPath("4.0/topics.json")
            .withParam("id", "cbbh"));
        
        assertTrue(q.isListQuery());
        assertThat(q.getOperands().size(), is(1));
        AttributeQuery<Id> operand = (AttributeQuery<Id>) Iterables.getOnlyElement(q.getOperands());
        assertThat(operand.getValue(), hasItem(Id.valueOf(idCodec.decode("cbbh"))));
        
        verify(queryContextParser).parseListContext(isA(HttpServletRequest.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testParsesAttributeKeyWithOperator() throws Exception {
        when(queryContextParser.parseListContext(isA(HttpServletRequest.class)))
            .thenReturn(QueryContext.standard(mock(HttpServletRequest.class)));
        
        Query<Topic> q = queryParser.parse(requestWithPath("4.0/topics.json")
                .withParam("aliases.namespace.beginning", "prefix"));
        
        assertTrue(q.isListQuery());
        assertThat(q.getOperands().size(), is(1));
        AttributeQuery<String> operand = (AttributeQuery<String>) Iterables.getOnlyElement(q.getOperands());
        assertThat(operand.getValue(), hasItem("prefix"));
        
        verify(queryContextParser).parseListContext(isA(HttpServletRequest.class));
    }



    @Test(expected=InvalidParameterException.class)
    public void testRejectsInvalidAttributeKey() throws Exception {
        when(queryContextParser.parseListContext(isA(HttpServletRequest.class)))
        .thenReturn(QueryContext.standard(mock(HttpServletRequest.class)));
        
        queryParser.parse(requestWithPath("4.0/topics.json")
                .withParam("just.the.beginning", "prefix"));
        
    }
    
    @Test(expected=InvalidOperatorException.class)
    public void testRejectsAttributeKeyWithBadOperator() throws Exception {
        when(queryContextParser.parseListContext(isA(HttpServletRequest.class)))
        .thenReturn(QueryContext.standard(mock(HttpServletRequest.class)));
        
        queryParser.parse(requestWithPath("4.0/topics.json")
                .withParam("aliases.namespace.ending", "suffix"));
        
    }

    private StubHttpServletRequest requestWithPath(String uri) {
        return new StubHttpServletRequest().withRequestUri(uri);
    }

}
