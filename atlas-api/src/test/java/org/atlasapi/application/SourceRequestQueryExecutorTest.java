package org.atlasapi.application;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.application.users.Role;
import org.atlasapi.application.users.User;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.entity.Id;
import org.atlasapi.output.useraware.UserAwareQueryResult;
import org.atlasapi.query.annotation.ActiveAnnotations;
import org.atlasapi.query.common.QueryExecutionException;
import org.atlasapi.query.common.useraware.UserAwareQuery;
import org.atlasapi.query.common.useraware.UserAwareQueryContext;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SourceRequestQueryExecutorTest {

    private SourceRequestQueryExecutor executor;
    private SourceRequest sourceRequest1;
    private SourceRequest sourceRequest2;

    @Mock SourceRequestStore store;

    @Before
    public void setUp() {
        sourceRequest1 = SourceRequest.builder()
                .withAppId(Id.valueOf(5000))
                .withEmail("me@example.com")
                .withApproved(false)
                .withReason("reason 1")
                .withUsageType(UsageType.COMMERCIAL)
                .build();
        sourceRequest2 = SourceRequest.builder()
                .withAppId(Id.valueOf(6000))
                .withEmail("other@example.com")
                .withApproved(false)
                .withReason("reason 2")
                .withUsageType(UsageType.COMMERCIAL)
                .build();
        when(store.sourceRequestFor(Id.valueOf(5000))).thenReturn(Optional.of(sourceRequest1));
        when(store.sourceRequestFor(Id.valueOf(6000))).thenReturn(Optional.of(sourceRequest2));
        when(store.all()).thenReturn(ImmutableSet.of(sourceRequest1, sourceRequest2));
        executor = new SourceRequestQueryExecutor(store);
    }

    @Test
    public void testExecutingAllSourceRequestQuery() throws QueryExecutionException {
        User user = User.builder().withId(Id.valueOf(5000)).withRole(Role.ADMIN).build();
        UserAwareQueryContext context = new UserAwareQueryContext(
                ApplicationSources.defaults(),
                ActiveAnnotations.standard(),
                Optional.of(user),
                mock(HttpServletRequest.class)
        );
        AttributeQuerySet emptyAttributeQuerySet = new AttributeQuerySet(ImmutableSet.<AttributeQuery<?>>of());
        UserAwareQuery<SourceRequest> query = UserAwareQuery.listQuery(
                emptyAttributeQuerySet,
                context
        );
        UserAwareQueryResult<SourceRequest> result = executor.execute(query);
        assertTrue(result.isListResult());
        assertTrue(result.getResources().contains(sourceRequest1));
    }
}
