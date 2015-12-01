package org.atlasapi.query.v4.organisation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.organisation.Organisation;
import org.atlasapi.organisation.OrganisationResolver;
import org.atlasapi.query.common.Query;
import org.atlasapi.query.common.QueryContext;
import org.atlasapi.query.common.QueryResult;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;

@RunWith(MockitoJUnitRunner.class)
public class OrganisationQueryExecutorTest {
    @Mock
    private OrganisationResolver organisationResolver;

    @InjectMocks
    private OrganisationQueryExecutor objectUnderTest;


    @Test
    public void testExecuteSingle() throws Exception {
        Id organisationId = Id.valueOf(1L);
        Organisation result = mock(Organisation.class);
        QueryContext context = mock(QueryContext.class);
        Query<Organisation> organisationQuery = mock(Query.class);
        when(organisationQuery.isListQuery()).thenReturn(false);
        when(organisationQuery.getOnlyId()).thenReturn(organisationId);
        when(organisationQuery.getContext()).thenReturn(context);
        when(organisationResolver.resolveIds((Iterable<Id>) argThat(containsInAnyOrder(organisationId))))
                .thenReturn(
                        Futures.immediateFuture(
                                Resolved.valueOf(ImmutableSet.of(result))
                        )
                );


        QueryResult<Organisation> queryResult = objectUnderTest.execute(organisationQuery);

        assertThat(queryResult.getOnlyResource(), is(result));
    }


    @Test(expected = UnsupportedOperationException.class)
    public void testExecuteMultiFails() throws Exception {
        Query<Organisation> organisationQuery = mock(Query.class);
        when(organisationQuery.isListQuery()).thenReturn(true);
        objectUnderTest.execute(organisationQuery);
    }
}