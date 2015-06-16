package org.atlasapi.query.v4.topic;

import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.Set;

import org.atlasapi.application.ApplicationSources;
import org.atlasapi.application.SourceStatus;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentIndex;
import org.atlasapi.content.Episode;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.equivalence.MergingEquivalentsResolver;
import org.atlasapi.equivalence.ResolvedEquivalents;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.query.annotation.ActiveAnnotations;
import org.atlasapi.query.common.ContextualQuery;
import org.atlasapi.query.common.ContextualQueryResult;
import org.atlasapi.query.common.ForbiddenException;
import org.atlasapi.query.common.Query;
import org.atlasapi.query.common.Query.ListQuery;
import org.atlasapi.query.common.Query.SingleQuery;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryExecutionException;
import org.atlasapi.topic.Topic;
import org.atlasapi.topic.TopicResolver;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.metabroadcast.common.query.Selection;

import javax.servlet.http.HttpServletRequest;

@RunWith(MockitoJUnitRunner.class)
public class TopicContentQueryExecutorTest {

    private @Mock TopicResolver topicResolver;
    private @Mock ContentIndex contentIndex;
    private @Mock MergingEquivalentsResolver<Content> equivalentsResolver;
    
    private TopicContentQueryExecutor executor;
    
    @Before
    public void setup() {
        executor = new TopicContentQueryExecutor(topicResolver, contentIndex, equivalentsResolver); 
    }
    
    @Test
    public void testExecutingTopicContentQuery() throws QueryExecutionException {
        
        AttributeQuerySet emptyAttributeQuerySet = new AttributeQuerySet(ImmutableSet.<AttributeQuery<?>>of());
        QueryContext context = new QueryContext(ApplicationSources.defaults()
                .copyWithChangedReadableSourceStatus(Publisher.BBC, SourceStatus.AVAILABLE_ENABLED)
                .copyWithChangedReadableSourceStatus(Publisher.DBPEDIA, SourceStatus.AVAILABLE_ENABLED), ActiveAnnotations.standard(), mock(HttpServletRequest.class));
        SingleQuery<Topic> contextQuery = Query.singleQuery(Id.valueOf(1234), context );
        ListQuery<Content> resourceQuery = Query.listQuery(emptyAttributeQuerySet, context);

        Topic topic = new Topic();
        topic.setId(Id.valueOf(1234));
        topic.setPublisher(Publisher.DBPEDIA);
        Content content = new Episode();
        content.setId(Id.valueOf(1235));
        content.setPublisher(Publisher.METABROADCAST);
        
        when(topicResolver.resolveIds(argThat(hasItems(topic.getId()))))
            .thenReturn(Futures.immediateFuture(Resolved.valueOf(ImmutableSet.of(topic))));
        when(contentIndex.query(emptyAttributeQuerySet, context.getApplicationSources().getEnabledReadSources(), Selection.all(),
                Optional.empty(), Optional.empty()))
            .thenReturn(Futures.immediateFuture(FluentIterable.from(ImmutableSet.of(content.getId()))));
        when(equivalentsResolver.resolveIds(argThat(hasItems(content.getId())), argThat(is(context.getApplicationSources())), argThat(is(context.getAnnotations().all()))))
            .thenReturn(Futures.immediateFuture(ResolvedEquivalents.<Content>builder().putEquivalents(Id.valueOf(1235), ImmutableSet.of(content)).build()));
        
        ContextualQueryResult<Topic,Content> result
            = executor.execute(ContextualQuery.valueOf(contextQuery, resourceQuery, context));
        
        assertThat(result.getContextResult().getOnlyResource(), is(topic));
        assertThat(result.getResourceResult().getResources().first().get(), is(content));
        assertThat(result.getContext(), is(context));
    }

    @Test(expected=NotFoundException.class)
    public void testFailsWhenTopicIsMissing() throws Throwable {
        
        AttributeQuerySet emptyAttributeQuerySet = new AttributeQuerySet(ImmutableSet.<AttributeQuery<?>>of());
        QueryContext context = QueryContext.standard(mock(HttpServletRequest.class));
        SingleQuery<Topic> contextQuery = Query.singleQuery(Id.valueOf(1234), context );
        ListQuery<Content> resourceQuery = Query.listQuery(emptyAttributeQuerySet, context);
        
        when(topicResolver.resolveIds(argThat(hasItems(Id.valueOf(1234)))))
            .thenReturn(Futures.immediateFuture(Resolved.<Topic>empty()));
        
        try {
            executor.execute(ContextualQuery.valueOf(contextQuery, resourceQuery, context));
        } catch (QueryExecutionException qee) {
            verify(contentIndex, never()).query(argThat(isA(AttributeQuerySet.class)), argThat(isA(Iterable.class)), argThat(isA(Selection.class)),
                    argThat(isA(Optional.class)), Optional.empty());
            verify(equivalentsResolver, never()).resolveIds(argThat(isA(Iterable.class)), argThat(isA(ApplicationSources.class)), argThat(isA(Set.class)));
            throw qee.getCause();
        }
        
    }

    @Test(expected=ForbiddenException.class)
    public void testFailsWhenTopicIsForbidden() throws Throwable {
        
        AttributeQuerySet emptyAttributeQuerySet = new AttributeQuerySet(ImmutableSet.<AttributeQuery<?>>of());
        QueryContext context = QueryContext.standard(mock(HttpServletRequest.class));
        SingleQuery<Topic> contextQuery = Query.singleQuery(Id.valueOf(1234), context );
        ListQuery<Content> resourceQuery = Query.listQuery(emptyAttributeQuerySet, context);
        
        Topic topic = new Topic();
        topic.setId(Id.valueOf(1234));
        topic.setPublisher(Publisher.PA);
        
        when(topicResolver.resolveIds(argThat(hasItems(Id.valueOf(1234)))))
            .thenReturn(Futures.immediateFuture(Resolved.valueOf(ImmutableSet.of(topic))));
        
        try {
            executor.execute(ContextualQuery.valueOf(contextQuery, resourceQuery, context));
        } catch (QueryExecutionException qee) {
            verify(contentIndex, never()).query(argThat(isA(AttributeQuerySet.class)), argThat(isA(Iterable.class)), argThat(isA(Selection.class)),
                    argThat(isA(Optional.class)), Optional.empty());
            verify(equivalentsResolver, never()).resolveIds(argThat(isA(Iterable.class)), argThat(isA(ApplicationSources.class)), argThat(isA(Set.class)));
            throw qee.getCause();
        }
        
    }
}
