package org.atlasapi.system.legacy;

import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.util.Iterator;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;

@RunWith(MockitoJUnitRunner.class)
public class LegacyLookupResolvingContentListerTest {

    private @Mock LookupEntryStore lookupEntryStore;
    private @Mock KnownTypeContentResolver contentResolver;

    private ContentListingCriteria criteria;

    private Item item;
    private Person person;

    private LookupEntry itemEntry;
    private LookupEntry personEntry;

    private LegacyContentLister contentLister;

    @Before
    public void setUp() throws Exception {
        contentLister = new LegacyLookupResolvingContentLister(lookupEntryStore,  contentResolver);

        Publisher publisher = Publisher.BBC;

        criteria = ContentListingCriteria.defaultCriteria()
                .forPublisher(Publisher.BBC)
                .build();

        item = new Item("uriA", "uriA", publisher);
        person = new Person("uriB", "uriB", publisher);

        itemEntry = LookupEntry.lookupEntryFrom(item);
        personEntry = LookupEntry.lookupEntryFrom(person);
    }

    @Test
    public void testListContent() throws Exception {
        when(lookupEntryStore.allEntriesForPublishers(criteria))
                .thenReturn(ImmutableList.of(itemEntry));
        when(contentResolver.findByLookupRefs(ImmutableList.of(itemEntry.lookupRef())))
                .thenReturn(
                        ResolvedContent.builder()
                                .put(itemEntry.uri(), item)
                                .build()
                );

        Iterator<Content> content = contentLister.listContent(criteria);

        assertThat(content.hasNext(), is(true));
        assertThat(content.next(), sameInstance(item));
    }

    @Test
    public void testListContentFiltersNonContent() throws Exception {
        when(lookupEntryStore.allEntriesForPublishers(criteria))
                .thenReturn(ImmutableList.of(personEntry));
        when(contentResolver.findByLookupRefs(ImmutableList.of(personEntry.lookupRef())))
                .thenReturn(
                        ResolvedContent.builder()
                                .put(personEntry.uri(), person)
                                .build()
                );

        Iterator<Content> content = contentLister.listContent(criteria);

        assertThat(content.hasNext(), is(false));
    }

    @Test
    public void testListContentFiltersNonResolvingLookupEntries() throws Exception {
        when(lookupEntryStore.allEntriesForPublishers(criteria))
                .thenReturn(ImmutableList.of(itemEntry));
        when(contentResolver.findByLookupRefs(ImmutableList.of(itemEntry.lookupRef())))
                .thenReturn(ResolvedContent.builder().build());

        Iterator<Content> content = contentLister.listContent(criteria);

        assertThat(content.hasNext(), is(false));
    }
}