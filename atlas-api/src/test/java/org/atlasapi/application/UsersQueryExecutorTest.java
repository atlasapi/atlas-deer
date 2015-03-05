package org.atlasapi.application;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.atlasapi.application.users.Role;
import org.atlasapi.application.users.User;
import org.atlasapi.application.users.UserStore;
import org.atlasapi.application.users.UsersQueryExecutor;
import org.atlasapi.entity.Id;
import org.atlasapi.output.ResourceForbiddenException;
import org.atlasapi.output.useraware.UserAwareQueryResult;
import org.atlasapi.query.annotation.ActiveAnnotations;
import org.atlasapi.query.common.useraware.UserAwareQuery;
import org.atlasapi.query.common.useraware.UserAwareQueryContext;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Optional;

import javax.servlet.http.HttpServletRequest;


public class UsersQueryExecutorTest {
    
    private UsersQueryExecutor executor;
    
    @Before
    public void setUp() {
        User user1 = User.builder()
                .withId(Id.valueOf(5000))
                .withFullName("user1")
                .build();
        User user2 = User.builder()
                .withId(Id.valueOf(6000))
                .withFullName("user2")
                .build();
        UserStore userStore = mock(UserStore.class);
        when(userStore.userForId(Id.valueOf(5000))).thenReturn(Optional.of(user1));
        when(userStore.userForId(Id.valueOf(6000))).thenReturn(Optional.of(user2));
        executor = new UsersQueryExecutor(userStore);
    }
    
    @Test
    public void testExecutingUserQuery() throws Exception {
        User user = User.builder().withId(Id.valueOf(5000)).withRole(Role.ADMIN).build();
        UserAwareQueryContext context = new UserAwareQueryContext(ApplicationSources.defaults(), 
                ActiveAnnotations.standard(),
                Optional.of(user),
                mock(HttpServletRequest.class));
        UserAwareQuery<User> query = UserAwareQuery.singleQuery(Id.valueOf(6000), context);
        UserAwareQueryResult<User> result = executor.execute(query);
        assertFalse(result.isListResult());
        assertEquals(result.getOnlyResource().getId(), Id.valueOf(6000));
        assertEquals(result.getOnlyResource().getFullName(), "user2");
    }
    
    /**
     * Make sure a regular user can see their own profile
     * @throws Exception
     */
    @Test
    public void testCanSeeOwnProfile() throws Exception {
        User user = User.builder().withId(Id.valueOf(5000)).withRole(Role.REGULAR).build();
        UserAwareQueryContext context = new UserAwareQueryContext(ApplicationSources.defaults(), 
                ActiveAnnotations.standard(),
                Optional.of(user),
                mock(HttpServletRequest.class));
        UserAwareQuery<User> query = UserAwareQuery.singleQuery(Id.valueOf(5000), context);
        UserAwareQueryResult<User> result = executor.execute(query);
        assertFalse(result.isListResult());
        assertEquals(result.getOnlyResource().getId(), Id.valueOf(5000));
        assertEquals(result.getOnlyResource().getFullName(), "user1");
    }
    
    /**
     * Make sure a regular user cannot see someone else's profile
     * @throws Exception
     */
    @Test(expected=ResourceForbiddenException.class)
    public void testCannotSeeOtherProfile() throws Exception {
        User user = User.builder().withId(Id.valueOf(5000)).withRole(Role.REGULAR).build();
        UserAwareQueryContext context = new UserAwareQueryContext(ApplicationSources.defaults(), 
                ActiveAnnotations.standard(),
                Optional.of(user),
                mock(HttpServletRequest.class));
        UserAwareQuery<User> query = UserAwareQuery.singleQuery(Id.valueOf(6000), context);
        executor.execute(query);
    }

}
