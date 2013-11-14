package org.atlasapi.model.translators;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

import java.util.Set;

import org.atlasapi.application.Application;
import org.atlasapi.application.LegacyApplicationStore;
import org.atlasapi.application.users.Role;
import org.atlasapi.application.users.User;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.social.model.UserRef;
import com.metabroadcast.common.social.model.UserRef.UserNamespace;

public class UserModelTranslatorTest {
	private static final Id USER_ID = Id.valueOf(5000);
	private static final UserRef USER_REF = new UserRef(6000, UserNamespace.TWITTER, "test"); 
	private static final String SCREEN_NAME = "testUser";
	private static final String FULL_NAME = "Full Name";
	private static final String COMPANY = "Compnay";
	private static final String EMAIL = "me@example.com";
	private static final String WEBSITE = "http://www.example.com";
	private static final String PROFILE_IMAGE = "http://www.example.com/image.png";
	private static final Role ROLE = Role.ADMIN;
	private static final Set<Publisher> SOURCES = ImmutableSet.of(Publisher.ARCHIVE_ORG, Publisher.DBPEDIA);
    private static final boolean PROFILE_COMPLETE = true;
    private static final Set<String> APP_SLUGS = ImmutableSet.of("app1", "app2");
    private static final Set<Id> APP_IDS = ImmutableSet.of(Id.valueOf(7000), Id.valueOf(8000));
    
    private LegacyApplicationStore store;
    
    @Before
    public void setUp() {
    	store = mock(LegacyApplicationStore.class);
    	when(store.applicationIdsForSlugs(APP_SLUGS)).thenReturn(APP_IDS);
    	Application app1 = Application.builder().withSlug("app1").withId(Id.valueOf(7000)).build();
    	Application app2 = Application.builder().withSlug("app2").withId(Id.valueOf(8000)).build();
    	when(store.applicationsFor(APP_IDS)).thenReturn(ImmutableList.of(app1, app2));
    }
	
	@Test
	public void test3To4UserTranslation() {
		org.atlasapi.application.users.v3.User user = org.atlasapi.application.users.v3.User.builder()
        .withId(USER_ID.longValue())
        .withUserRef(USER_REF)
        .withScreenName(SCREEN_NAME)
        .withFullName(FULL_NAME)
        .withCompany(COMPANY)
        .withEmail(EMAIL)
        .withWebsite(WEBSITE)
        .withProfileImage(PROFILE_IMAGE)
        .withRole(ROLE)
        .withApplicationSlugs(APP_SLUGS)
        .withSources(SOURCES)
        .withProfileComplete(PROFILE_COMPLETE)
        .build();
		
		UserModelTranslator translator = new UserModelTranslator(store);
		User result = translator.transform3to4(user);
		assertEquals(USER_ID, result.getId());
		assertEquals(USER_REF, result.getUserRef());
		assertEquals(SCREEN_NAME, result.getScreenName());
		assertEquals(FULL_NAME, result.getFullName());
		assertEquals(COMPANY, result.getCompany());
		assertEquals(EMAIL, result.getEmail());
		assertEquals(WEBSITE, result.getWebsite());
		assertEquals(PROFILE_IMAGE, result.getProfileImage());
		assertEquals(ROLE, result.getRole());
		assertTrue(result.getApplicationIds().containsAll(APP_IDS));
		assertTrue(result.getSources().containsAll(SOURCES));
		assertTrue(result.isProfileComplete());
		
	}
	
	@Test
	public void test4To3UserTranslation() {
		User user = User.builder()
        .withId(USER_ID)
        .withUserRef(USER_REF)
        .withScreenName(SCREEN_NAME)
        .withFullName(FULL_NAME)
        .withCompany(COMPANY)
        .withEmail(EMAIL)
        .withWebsite(WEBSITE)
        .withProfileImage(PROFILE_IMAGE)
        .withApplicationIds(APP_IDS)
        .withSources(SOURCES)
        .withRole(ROLE)
        .withProfileComplete(PROFILE_COMPLETE)
        .build();
		
		UserModelTranslator translator = new UserModelTranslator(store);
		org.atlasapi.application.users.v3.User result = translator.transform4to3(user);
		assertEquals(Long.valueOf(USER_ID.longValue()), result.getId());
		assertEquals(USER_REF, result.getUserRef());
		assertEquals(SCREEN_NAME, result.getScreenName());
		assertEquals(FULL_NAME, result.getFullName());
		assertEquals(COMPANY, result.getCompany());
		assertEquals(WEBSITE, result.getWebsite());
		assertEquals(PROFILE_IMAGE, result.getProfileImage());
		assertEquals(ROLE, result.getRole());
		assertTrue(result.getApplicationSlugs().containsAll(APP_SLUGS));
		assertTrue(result.getSources().containsAll(SOURCES));
		assertTrue(result.isProfileComplete());
	}
}
