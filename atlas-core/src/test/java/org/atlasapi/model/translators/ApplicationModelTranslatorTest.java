package org.atlasapi.model.translators;

import java.util.List;
import java.util.Map;

import org.atlasapi.application.Application;
import org.atlasapi.application.ApplicationCredentials;
import org.atlasapi.application.ApplicationSources;
import org.atlasapi.application.SourceReadEntry;
import org.atlasapi.application.SourceStatus;
import org.atlasapi.legacy.application.ApplicationConfiguration;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.time.DateTimeZones;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.joda.time.DateTime;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ApplicationModelTranslatorTest {

    private static final String SLUG = "app-5000";
    private static final String TITLE = "test application";
    private static final String DESCRIPTION = "test description";
    private static final String API_KEY = "abc123";
    private static final DateTime CREATED = new DateTime(DateTimeZones.UTC)
            .withDate(2013, 9, 13)
            .withTime(15, 13, 0, 0);
    private static final SourceReadEntry READ_ENTRY_1 = new SourceReadEntry(
            Publisher.BBC,
            SourceStatus.AVAILABLE_ENABLED
    );
    private static final SourceReadEntry READ_ENTRY_2 = new SourceReadEntry(
            Publisher.ARCHIVE_ORG,
            SourceStatus.UNAVAILABLE
    );

    @Test
    public void test3To4Translation() {

        List<Publisher> writes = ImmutableList.of(Publisher.ARCHIVE_ORG, Publisher.DBPEDIA);
        Map<Publisher, org.atlasapi.application.v3.SourceStatus> sourceStatuses = Maps.newHashMap();
        sourceStatuses.put(
                Publisher.BBC,
                org.atlasapi.application.v3.SourceStatus.AVAILABLE_ENABLED
        );
        sourceStatuses.put(
                Publisher.ARCHIVE_ORG,
                org.atlasapi.application.v3.SourceStatus.UNAVAILABLE
        );
        ApplicationConfiguration configuration = ApplicationConfiguration.defaultConfiguration()
                .withSources(sourceStatuses);
        configuration = configuration.copyWithPrecedence(ImmutableList.of(
                Publisher.BBC,
                Publisher.ARCHIVE_ORG
        ));
        configuration = configuration.copyWithWritableSources(writes);

        org.atlasapi.legacy.application.ApplicationCredentials creds = new org.atlasapi.legacy.application.ApplicationCredentials(
                API_KEY);
        org.atlasapi.legacy.application.Application application = org.atlasapi.legacy.application.Application
                .application(SLUG)
                .withDeerId(Id.valueOf(5000).longValue())
                .withTitle(TITLE)
                .withDescription(DESCRIPTION)
                .createdAt(CREATED)
                .withConfiguration(configuration)
                .withCredentials(creds)
                .withRevoked(true)
                .build();
        ApplicationModelTranslator translator = new ApplicationModelTranslator();
        Application result = translator.apply(application);
        assertEquals(SLUG, result.getSlug());
        assertEquals(Id.valueOf(5000), result.getId());
        assertEquals(TITLE, result.getTitle());
        assertEquals(DESCRIPTION, result.getDescription());
        assertEquals(CREATED, result.getCreated());
        assertTrue(result.isRevoked());
        // Credentials
        assertEquals(API_KEY, result.getCredentials().getApiKey());
        // Sources
        assertTrue(result.getSources().isPrecedenceEnabled());
        assertTrue(result.getSources().isReadEnabled(Publisher.BBC));
        assertTrue(result.getSources().isWriteEnabled(Publisher.ARCHIVE_ORG));
    }

    @Test
    public void test4To3Translation() {
        ApplicationCredentials credentials = ApplicationCredentials.builder()
                .withApiKey(API_KEY).build();
        List<SourceReadEntry> reads = ImmutableList.of(READ_ENTRY_1, READ_ENTRY_2);
        List<Publisher> writes = ImmutableList.of(Publisher.ARCHIVE_ORG, Publisher.DBPEDIA);
        ApplicationSources sources = ApplicationSources.builder()
                .withPrecedence(true)
                .withReadableSources(reads)
                .withWritableSources(writes)
                .build();
        Application application = Application.builder()
                .withId(Id.valueOf(5000))
                .withSlug(SLUG)
                .withTitle(TITLE)
                .withCreated(CREATED)
                .withCredentials(credentials)
                .withSources(sources)
                .build();
        ApplicationModelTranslator translator = new ApplicationModelTranslator();
        org.atlasapi.legacy.application.Application result = translator.transform4to3(application);
        assertEquals(Long.valueOf(5000), result.getDeerId());
        assertEquals(SLUG, result.getSlug());
        assertEquals(CREATED, result.getCreated());
        // Credentials
        assertEquals(credentials.getApiKey(), result.getCredentials().getApiKey());
        // Sources
        assertTrue(result.getConfiguration().precedenceEnabled());
        assertTrue(result.getConfiguration().getEnabledSources().contains(Publisher.BBC));
        assertTrue(result.getConfiguration().canWrite(Publisher.ARCHIVE_ORG));
    }
}
