package org.atlasapi.application;

import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.atlasapi.application.users.Role;
import org.atlasapi.application.users.User;
import org.atlasapi.entity.Id;
import org.atlasapi.output.ResourceForbiddenException;
import org.atlasapi.output.useraware.UserAwareQueryResult;
import org.atlasapi.query.annotation.ActiveAnnotations;
import org.atlasapi.query.common.useraware.UserAwareQuery;
import org.atlasapi.query.common.useraware.UserAwareQueryContext;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ApplicationQueryExecutorTest {

    private ApplicationQueryExecutor executor;

    @Before
    public void setUp() {
        ApplicationStore store = mock(ApplicationStore.class);
        Application application1 = Application.builder()
                .withId(Id.valueOf(5000))
                .withTitle("test 1")
                .build();
        Application application2 = Application.builder()
                .withId(Id.valueOf(6000))
                .withTitle("test 2")
                .build();
        when(store.applicationFor(Id.valueOf(5000))).thenReturn(Optional.of(application1));
        when(store.applicationFor(Id.valueOf(6000))).thenReturn(Optional.of(application2));
        when(store.allApplications()).thenReturn(ImmutableList.of(application1, application2));
        executor = new ApplicationQueryExecutor(store);
    }

    @Test
    public void testExecutingApplicationQuery() throws Exception {
        User user = User.builder().withId(Id.valueOf(5000)).withRole(Role.ADMIN).build();
        UserAwareQueryContext context = new UserAwareQueryContext(
                ApplicationSources.defaults(),
                ActiveAnnotations.standard(),
                Optional.of(user),
                mock(HttpServletRequest.class)
        );
        UserAwareQuery<Application> query = UserAwareQuery.singleQuery(Id.valueOf(5000), context);
        UserAwareQueryResult<Application> result = executor.execute(query);
        assertFalse(result.isListResult());
        assertEquals(result.getOnlyResource().getId(), Id.valueOf(5000));
    }

    /**
     * Make sure a reguar user can see their own application
     *
     * @throws Exception
     */
    @Test
    public void testCanSeeOwnApplication() throws Exception {
        Set<Id> appIds = ImmutableSet.of(Id.valueOf(5000));
        User user = User.builder()
                .withId(Id.valueOf(5000))
                .withApplicationIds(appIds)
                .withRole(Role.REGULAR)
                .build();
        UserAwareQueryContext context = new UserAwareQueryContext(
                ApplicationSources.defaults(),
                ActiveAnnotations.standard(),
                Optional.of(user),
                mock(HttpServletRequest.class)
        );
        UserAwareQuery<Application> query = UserAwareQuery.singleQuery(Id.valueOf(5000), context);
        UserAwareQueryResult<Application> result = executor.execute(query);
        assertFalse(result.isListResult());
        assertEquals(result.getOnlyResource().getId(), Id.valueOf(5000));
    }

    /**
     * Make sure a regular user cannot see an application that they do not own
     *
     * @throws Exception
     */
    @Test(expected = ResourceForbiddenException.class)
    public void testCannotSeeOtherUserApplication() throws Exception {
        Set<Id> appIds = ImmutableSet.of(Id.valueOf(5000));
        User user = User.builder()
                .withId(Id.valueOf(5000))
                .withApplicationIds(appIds)
                .withRole(Role.REGULAR)
                .build();
        UserAwareQueryContext context = new UserAwareQueryContext(
                ApplicationSources.defaults(),
                ActiveAnnotations.standard(),
                Optional.of(user),
                mock(HttpServletRequest.class)
        );
        UserAwareQuery<Application> query = UserAwareQuery.singleQuery(Id.valueOf(6000), context);
        executor.execute(query);
    }

    /**
     * Make sure an admin can see an application even if they do not own it
     *
     * @throws Exception
     */
    @Test
    public void testAdminCanSeeOtherUserApplication() throws Exception {
        Set<Id> appIds = ImmutableSet.of(Id.valueOf(5000));
        User user = User.builder()
                .withId(Id.valueOf(5000))
                .withApplicationIds(appIds)
                .withRole(Role.ADMIN)
                .build();
        UserAwareQueryContext context = new UserAwareQueryContext(
                ApplicationSources.defaults(),
                ActiveAnnotations.standard(),
                Optional.of(user),
                mock(HttpServletRequest.class)
        );
        UserAwareQuery<Application> query = UserAwareQuery.singleQuery(Id.valueOf(6000), context);
        UserAwareQueryResult<Application> result = executor.execute(query);
        assertFalse(result.isListResult());
        assertEquals(result.getOnlyResource().getId(), Id.valueOf(6000));
    }
}
