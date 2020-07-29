package org.atlasapi.query.v4.topic;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentSearcher;
import org.atlasapi.content.Episode;
import org.atlasapi.content.IndexQueryResult;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.equivalence.MergingEquivalentsResolver;
import org.atlasapi.equivalence.ResolvedEquivalents;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.NotFoundException;
import org.atlasapi.query.annotation.ActiveAnnotations;
import org.atlasapi.query.common.ContextualQuery;
import org.atlasapi.query.common.ContextualQueryResult;
import org.atlasapi.query.common.Query;
import org.atlasapi.query.common.Query.ListQuery;
import org.atlasapi.query.common.Query.SingleQuery;
import org.atlasapi.query.common.context.QueryContext;
import org.atlasapi.query.common.exceptions.ForbiddenException;
import org.atlasapi.query.common.exceptions.QueryExecutionException;
import org.atlasapi.topic.Topic;
import org.atlasapi.topic.TopicResolver;

import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.applications.client.model.internal.ApplicationConfiguration;
import com.metabroadcast.common.query.Selection;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TopicContentQueryExecutorTest {

    private @Mock TopicResolver topicResolver;
    private @Mock ContentSearcher contentIndex;
    private @Mock MergingEquivalentsResolver<Content> equivalentsResolver;
    private @Mock Application application;
    private @Mock HttpServletRequest request;

    private TopicContentQueryExecutor executor;
    private QueryContext queryContext;

    @Before
    public void setup() {
        when(application.getConfiguration()).thenReturn(
                ApplicationConfiguration.builder()
                        .withNoPrecedence(getPublishers())
                        .withEnabledWriteSources(ImmutableList.of())
                        .build()
        );

        executor = TopicContentQueryExecutor.create(topicResolver, contentIndex, equivalentsResolver);
        queryContext = QueryContext.create(application, ActiveAnnotations.standard(), request);
    }

    @Test
    public void testExecutingTopicContentQuery() throws QueryExecutionException {

        QueryContext context = QueryContext.create(
                application,
                ActiveAnnotations.standard(),
                ImmutableSet.of(),
                mock(HttpServletRequest.class)
        );
        SingleQuery<Topic> contextQuery = Query.singleQuery(Id.valueOf(1234), context);
        ListQuery<Content> resourceQuery = Query.listQuery(ImmutableSet.of(), context);

        Topic topic = new Topic();
        topic.setId(Id.valueOf(1234));
        topic.setPublisher(Publisher.DBPEDIA);
        Content content = new Episode();
        content.setId(Id.valueOf(1235));
        content.setPublisher(Publisher.METABROADCAST);

        when(topicResolver.resolveIds(argThat(hasItems(topic.getId()))))
                .thenReturn(Futures.immediateFuture(Resolved.valueOf(ImmutableSet.of(topic))));
        FluentIterable<Id> returning = FluentIterable.from(ImmutableSet.of(content.getId()));
        when(contentIndex.query(
                ImmutableSet.of(),
                context.getApplication().getConfiguration().getEnabledReadSources(),
                Selection.all()
        ))
                .thenReturn(Futures.immediateFuture(IndexQueryResult.withIds(returning, 0L)));
        when(equivalentsResolver.resolveIds(
                argThat(hasItems(content.getId())),
                argThat(is(context.getApplication())),
                argThat(is(context.getAnnotations().all())),
                argThat(is(ImmutableSet.of()))
        ))
                .thenReturn(Futures.immediateFuture(ResolvedEquivalents.<Content>builder().putEquivalents(
                        Id.valueOf(1235),
                        ImmutableSet.of(content)
                ).build()));

        ContextualQueryResult<Topic, Content> result
                = executor.execute(ContextualQuery.valueOf(contextQuery, resourceQuery, context));

        assertThat(result.getContextResult().getOnlyResource(), is(topic));
        assertThat(result.getResourceResult().getResources().first().get(), is(content));
        assertThat(result.getContext(), is(context));
    }

    @Test(expected = NotFoundException.class)
    public void testFailsWhenTopicIsMissing() throws Throwable {

        SingleQuery<Topic> contextQuery = Query.singleQuery(Id.valueOf(1234), queryContext);
        ListQuery<Content> resourceQuery = Query.listQuery(ImmutableSet.of(), queryContext);

        when(topicResolver.resolveIds(argThat(hasItems(Id.valueOf(1234)))))
                .thenReturn(Futures.immediateFuture(Resolved.<Topic>empty()));

        try {
            executor.execute(ContextualQuery.valueOf(contextQuery, resourceQuery, queryContext));
        } catch (QueryExecutionException qee) {
           verifyException(qee);
        }

    }

    @Test(expected = ForbiddenException.class)
    public void testFailsWhenTopicIsForbidden() throws Throwable {

        SingleQuery<Topic> contextQuery = Query.singleQuery(Id.valueOf(1234), queryContext);
        ListQuery<Content> resourceQuery = Query.listQuery(ImmutableSet.of(), queryContext);

        Topic topic = new Topic();
        topic.setId(Id.valueOf(1234));
        topic.setPublisher(Publisher.PA);

        when(topicResolver.resolveIds(argThat(hasItems(Id.valueOf(1234)))))
                .thenReturn(Futures.immediateFuture(Resolved.valueOf(ImmutableSet.of(topic))));

        try {
            executor.execute(ContextualQuery.valueOf(contextQuery, resourceQuery, queryContext));
        } catch (QueryExecutionException qee) {
            verifyException(qee);
        }

    }

    private void verifyException(QueryExecutionException qee) throws Throwable {
        verify(contentIndex, never()).query(
                argThat(isA(Iterable.class)),
                argThat(isA(Iterable.class)),
                argThat(isA(Selection.class))
        );
        verify(equivalentsResolver, never()).resolveIds(
                argThat(isA(Iterable.class)),
                argThat(isA(Application.class)),
                argThat(isA(Set.class)),
                argThat(isA(Set.class))
        );
        throw qee.getCause();
    }

    private List<Publisher> getPublishers() {
        return ImmutableList.<Publisher>builder()
                .addAll(
                        Publisher.all().stream()
                                .filter(Publisher::enabledWithNoApiKey)
                                .collect(Collectors.toList())
                )
                .add(Publisher.DBPEDIA)
                .build();
    }
}
