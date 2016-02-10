package org.atlasapi.application;

import java.util.List;

import org.atlasapi.application.SourceStatus.SourceState;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.time.DateTimeZones;

import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ApplicationModificationTest {

    private final String slug = "app-5000";
    private final String title = "test application";
    private final DateTime created = new DateTime(DateTimeZones.UTC)
            .withDate(2013, 9, 13)
            .withTime(15, 13, 0, 0);
    private final SourceReadEntry testEntry1 = new SourceReadEntry(
            Publisher.BBC,
            SourceStatus.AVAILABLE_ENABLED
    );
    private final SourceReadEntry testEntry2 = new SourceReadEntry(
            Publisher.NETFLIX,
            SourceStatus.UNAVAILABLE
    );

    @Test
    public void testRepaceSources() {
        ApplicationCredentials credentials = ApplicationCredentials.builder()
                .withApiKey("abc123").build();
        List<SourceReadEntry> reads = ImmutableList.of(testEntry1, testEntry2);
        List<Publisher> writes = ImmutableList.of(Publisher.KANDL_TOPICS, Publisher.DBPEDIA);
        ApplicationSources sources = ApplicationSources.builder()
                .withPrecedence(true)
                .withReadableSources(reads)
                .withWritableSources(writes)
                .build();
        Application application = Application.builder()
                .withId(Id.valueOf(5000))
                .withSlug(slug)
                .withTitle(title)
                .withCreated(created)
                .withCredentials(credentials)
                .withSources(sources)
                .build();
        List<SourceReadEntry> modifiedReads = ImmutableList.of(
                new SourceReadEntry(
                        Publisher.YOUTUBE,
                        SourceStatus.fromV3SourceStatus(Publisher.YOUTUBE.getDefaultSourceStatus())
                ),
                new SourceReadEntry(
                        Publisher.NETFLIX,
                        SourceStatus.fromV3SourceStatus(Publisher.NETFLIX.getDefaultSourceStatus())
                ),
                new SourceReadEntry(Publisher.BBC, SourceStatus.UNAVAILABLE)
        );

        List<Publisher> modfiedWrites = ImmutableList.of(
                Publisher.YOUTUBE,
                Publisher.ARCHIVE_ORG
        );
        ApplicationSources modifiedSources = ApplicationSources.builder()
                .withPrecedence(true)
                .withReadableSources(modifiedReads)
                .withWritableSources(modfiedWrites)
                .build();
        Application modified = application.copyWithSources(modifiedSources);
        //assertEquals(2, modified.getSources().getReads().size());
        assertEquals(Publisher.NETFLIX, modified.getSources().getReads().get(1).getPublisher());
        assertEquals(Publisher.ARCHIVE_ORG, modified.getSources().getWrites().get(1));
        assertEquals(
                SourceStatus.UNAVAILABLE,
                modified.getSources().readStatusOrDefault(Publisher.BBC)
        );
    }

    @Test
    public void testChangeReadSourceState() {
        ApplicationCredentials credentials = ApplicationCredentials.builder()
                .withApiKey("abc123").build();
        List<SourceReadEntry> reads = ImmutableList.of(testEntry1, testEntry2);
        List<Publisher> writes = ImmutableList.of(Publisher.KANDL_TOPICS, Publisher.DBPEDIA);
        ApplicationSources sources = ApplicationSources.builder()
                .withPrecedence(true)
                .withReadableSources(reads)
                .withWritableSources(writes)
                .build();
        Application application = Application.builder()
                .withId(Id.valueOf(5000))
                .withSlug(slug)
                .withTitle(title)
                .withCreated(created)
                .withCredentials(credentials)
                .withSources(sources)
                .build();
        Application modified = application.copyWithReadSourceState(
                Publisher.NETFLIX,
                SourceState.REQUESTED
        );

        //assertEquals(2, modified.getSources().getReads().size());
        assertEquals(
                SourceState.REQUESTED,
                modified.getSources().getReads().get(1).getSourceStatus().getState()
        );
    }

    @Test
    public void testEnableAndDisableSource() {
        ApplicationCredentials credentials = ApplicationCredentials.builder()
                .withApiKey("abc123").build();
        SourceReadEntry bbcDisabled = new SourceReadEntry(
                Publisher.BBC,
                SourceStatus.AVAILABLE_DISABLED
        );

        List<SourceReadEntry> reads = ImmutableList.of(bbcDisabled, testEntry2);
        List<Publisher> writes = ImmutableList.of(Publisher.KANDL_TOPICS, Publisher.DBPEDIA);
        ApplicationSources sources = ApplicationSources.builder()
                .withPrecedence(true)
                .withReadableSources(reads)
                .withWritableSources(writes)
                .build();
        Application application = Application.builder()
                .withId(Id.valueOf(5000))
                .withSlug(slug)
                .withTitle(title)
                .withCreated(created)
                .withCredentials(credentials)
                .withSources(sources)
                .build();
        Application modified = application.copyWithSourceEnabled(Publisher.BBC);
        assertTrue(modified.getSources().getReads().get(0).getSourceStatus().isEnabled());
        modified = application.copyWithSourceDisabled(Publisher.BBC);
        assertFalse(modified.getSources().getReads().get(0).getSourceStatus().isEnabled());
    }

    @Test
    public void testAddWriteSource() {
        ApplicationCredentials credentials = ApplicationCredentials.builder()
                .withApiKey("abc123").build();
        List<SourceReadEntry> reads = ImmutableList.of(testEntry1, testEntry2);
        List<Publisher> writes = ImmutableList.of(Publisher.KANDL_TOPICS, Publisher.DBPEDIA);
        ApplicationSources sources = ApplicationSources.builder()
                .withPrecedence(true)
                .withReadableSources(reads)
                .withWritableSources(writes)
                .build();
        Application application = Application.builder()
                .withId(Id.valueOf(5000))
                .withSlug(slug)
                .withTitle(title)
                .withCreated(created)
                .withCredentials(credentials)
                .withSources(sources)
                .build();
        Application modified = application.copyWithAddedWritingSource(Publisher.YOUTUBE);
        assertTrue(modified.getSources().getWrites().contains(Publisher.YOUTUBE));
    }

    @Test
    public void testRemoveWriteSource() {
        ApplicationCredentials credentials = ApplicationCredentials.builder()
                .withApiKey("abc123").build();
        List<SourceReadEntry> reads = ImmutableList.of(testEntry1, testEntry2);
        List<Publisher> writes = ImmutableList.of(Publisher.KANDL_TOPICS, Publisher.DBPEDIA);
        ApplicationSources sources = ApplicationSources.builder()
                .withPrecedence(true)
                .withReadableSources(reads)
                .withWritableSources(writes)
                .build();
        Application application = Application.builder()
                .withId(Id.valueOf(5000))
                .withSlug(slug)
                .withTitle(title)
                .withCreated(created)
                .withCredentials(credentials)
                .withSources(sources)
                .build();
        Application modified = application.copyWithRemovedWritingSource(Publisher.KANDL_TOPICS);
        assertFalse(modified.getSources().getWrites().contains(Publisher.KANDL_TOPICS));
    }

    @Test
    public void testPrecedence() {
        ApplicationCredentials credentials = ApplicationCredentials.builder()
                .withApiKey("abc123").build();
        List<SourceReadEntry> reads = ImmutableList.of(testEntry1, testEntry2);
        List<Publisher> writes = ImmutableList.of(Publisher.KANDL_TOPICS, Publisher.DBPEDIA);
        ApplicationSources sources = ApplicationSources.builder()
                .withPrecedence(true)
                .withReadableSources(reads)
                .withWritableSources(writes)
                .build();
        Application application = Application.builder()
                .withId(Id.valueOf(5000))
                .withSlug(slug)
                .withTitle(title)
                .withCreated(created)
                .withCredentials(credentials)
                .withSources(sources)
                .build();
        assertEquals(
                1,
                application.getSources()
                        .publisherPrecedenceOrdering()
                        .compare(Publisher.NETFLIX, Publisher.BBC)
        );
    }

    @Test
    public void testAddPrecedence() {
        ApplicationCredentials credentials = ApplicationCredentials.builder()
                .withApiKey("abc123").build();
        List<SourceReadEntry> reads = ImmutableList.of(testEntry1, testEntry2);
        List<Publisher> writes = ImmutableList.of(Publisher.KANDL_TOPICS, Publisher.DBPEDIA);
        ApplicationSources sources = ApplicationSources.builder()
                .withPrecedence(false)
                .withReadableSources(reads)
                .withWritableSources(writes)
                .build();
        Application application = Application.builder()
                .withId(Id.valueOf(5000))
                .withSlug(slug)
                .withTitle(title)
                .withCreated(created)
                .withCredentials(credentials)
                .withSources(sources)
                .build();
        Application modified = application.copyWithReadSourceOrder(ImmutableList.of(
                Publisher.NETFLIX,
                Publisher.BBC
        ));
        assertTrue(modified.getSources().isPrecedenceEnabled());
        assertEquals(Publisher.NETFLIX, modified.getSources().getReads().get(0).getPublisher());
    }

    @Test
    public void testRemovePrecedence() {
        ApplicationCredentials credentials = ApplicationCredentials.builder()
                .withApiKey("abc123").build();
        List<SourceReadEntry> reads = ImmutableList.of(testEntry1, testEntry2);
        List<Publisher> writes = ImmutableList.of(Publisher.KANDL_TOPICS, Publisher.DBPEDIA);
        ApplicationSources sources = ApplicationSources.builder()
                .withPrecedence(true)
                .withReadableSources(reads)
                .withWritableSources(writes)
                .build();
        Application application = Application.builder()
                .withId(Id.valueOf(5000))
                .withSlug(slug)
                .withTitle(title)
                .withCreated(created)
                .withCredentials(credentials)
                .withSources(sources)
                .build();
        Application modified = application.copyWithPrecedenceDisabled();
        assertFalse(modified.getSources().isPrecedenceEnabled());
    }

    @Test
    public void shouldCreateApplication() {
        assertNotNull(
                Application.builder()
                        .withSlug("test-slug")
                        .withCredentials(ApplicationCredentials.builder()
                                .withApiKey("apiKey")
                                .build())
                        .build()
        );
    }
}
