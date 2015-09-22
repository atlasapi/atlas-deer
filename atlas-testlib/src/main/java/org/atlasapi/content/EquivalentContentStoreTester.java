package org.atlasapi.content;

import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.equivalence.EquivalenceGraphUpdate;
import org.atlasapi.equivalence.ResolvedEquivalents;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.segment.SegmentEvent;
import org.joda.time.DateTime;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.testing.AbstractTester;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.time.DateTimeZones;

public final class EquivalentContentStoreTester extends AbstractTester<EquivalentContentStoreSubjectGenerator> {
    
    public void testWritingContentThatDoesntYetHaveAGraph() throws Exception {
        Content content = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        content.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        
        WriteResult<Content, Content> result = getSubjectGenerator().getContentStore().writeContent(content);
        assertTrue("Failed to write " + content, result.written());
        
        getSubjectGenerator().getEquivalentContentStore().updateContent(content);
        
        ResolvedEquivalents<Content> resolved
            = get(getSubjectGenerator().getEquivalentContentStore().resolveIds(ImmutableList.of(content.getId()), ImmutableSet.of(Publisher.METABROADCAST), Annotation.all()));
        
        ImmutableSet<Content> set = resolved.get(content.getId());
        assertThat(Iterables.getOnlyElement(set), is(content));
    }
    
    private <T> T get(ListenableFuture<T> resolveIds) throws Exception {
        return Futures.get(resolveIds, 10, TimeUnit.MINUTES, Exception.class);
    }

    public void testWritingAnEquivalenceGraph() throws Exception {
        Item content1 = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        content1.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        content1.setTitle("Two");
        content1.setSegmentEvents(ImmutableList.of(new SegmentEvent()));

        Content content2 = new Item(Id.valueOf(2), Publisher.BBC);
        content2.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        content2.setTitle("Three");
        
        getSubjectGenerator().getContentStore().writeContent(content1);
        getSubjectGenerator().getContentStore().writeContent(content2);
        
        Optional<EquivalenceGraphUpdate> update
            = getSubjectGenerator().getEquivalenceGraphStore().updateEquivalences(content1.toRef(), ImmutableSet.<ResourceRef>of(content2.toRef()), 
                ImmutableSet.of(Publisher.METABROADCAST, Publisher.BBC));
        
        getSubjectGenerator().getEquivalentContentStore().updateEquivalences(update.get());
        
        ResolvedEquivalents<Content> resolved
            = get(getSubjectGenerator().getEquivalentContentStore().resolveIds(ImmutableList.of(content1.getId()), ImmutableSet.of(Publisher.METABROADCAST, Publisher.BBC), Annotation.all()));
    
        ImmutableSet<Content> set = resolved.get(content1.getId());
        assertThat(set, hasSize(2));
        assertThat(set, hasItems(content1, content2));
        assertThat(Iterables.getOnlyElement(((Item) set.asList().get(0)).getSegmentEvents()).getSource(), is(Publisher.METABROADCAST));
    }

    public void testUpdatingContentInAnEquivalenceGraph() throws Exception {
        Content content1 = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        content1.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        content1.setTitle("Two");
        
        Content content2 = new Item(Id.valueOf(2), Publisher.BBC);
        content2.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        content2.setTitle("Three");
        
        getSubjectGenerator().getContentStore().writeContent(content1);
        getSubjectGenerator().getContentStore().writeContent(content2);
        
        Optional<EquivalenceGraphUpdate> update
        = getSubjectGenerator().getEquivalenceGraphStore().updateEquivalences(content1.toRef(), ImmutableSet.<ResourceRef>of(content2.toRef()), 
                ImmutableSet.of(Publisher.METABROADCAST, Publisher.BBC));
        
        getSubjectGenerator().getEquivalentContentStore().updateEquivalences(update.get());
        
        String newTitle = "newTitle";
        content1.setTitle(newTitle);
        WriteResult<Content, Content> result = getSubjectGenerator().getContentStore().writeContent(content1);
        assertTrue("Failed to write " + content1, result.written());
        
        getSubjectGenerator().getEquivalentContentStore().updateContent(content1);
        
        ResolvedEquivalents<Content> resolved
            = get(getSubjectGenerator().getEquivalentContentStore().resolveIds(ImmutableList.of(content1.getId()), ImmutableSet.of(Publisher.METABROADCAST), Annotation.all()));
        
        ImmutableSet<Content> set = resolved.get(content1.getId());
        assertThat(Iterables.getOnlyElement(set).getTitle(), is(newTitle));
    }

    public void testResolvingTheSameSetSimultaneously() throws Exception {
        Content content1 = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        content1.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        content1.setTitle("Two");
        
        Content content2 = new Item(Id.valueOf(2), Publisher.BBC);
        content2.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        content2.setTitle("Three");
        
        getSubjectGenerator().getContentStore().writeContent(content1);
        getSubjectGenerator().getContentStore().writeContent(content2);
        
        Optional<EquivalenceGraphUpdate> update
        = getSubjectGenerator().getEquivalenceGraphStore().updateEquivalences(content1.toRef(), ImmutableSet.<ResourceRef>of(content2.toRef()), 
                ImmutableSet.of(Publisher.METABROADCAST, Publisher.BBC));
        
        getSubjectGenerator().getEquivalentContentStore().updateEquivalences(update.get());
        
        ResolvedEquivalents<Content> resolved
        = get(getSubjectGenerator().getEquivalentContentStore().resolveIds(ImmutableList.of(content1.getId(), content2.getId()), 
                ImmutableSet.of(Publisher.METABROADCAST), Annotation.all()));
        
        ImmutableSet<Content> set1 = resolved.get(content1.getId());
        ImmutableSet<Content> set2 = resolved.get(content2.getId());
        assertEquals("Sets resolved for ids of the same set should be equal", set1, set2);
    }

    public void testResolvingMissingId() throws Exception {
        Content content = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        content.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        
        WriteResult<Content, Content> result = getSubjectGenerator().getContentStore().writeContent(content);
        assertTrue("Failed to write " + content, result.written());
        
        getSubjectGenerator().getEquivalentContentStore().updateContent(content);
        
        ResolvedEquivalents<Content> resolved
            = get(getSubjectGenerator().getEquivalentContentStore().resolveIds(ImmutableList.of(Id.valueOf(2)), ImmutableSet.of(Publisher.METABROADCAST), Annotation.all()));
        
        assertTrue(resolved.get(content.getId()).isEmpty());
    }

    public void testResolvingIdOfUnavailableContentWhenResultingSetIsEmpty() throws Exception {
        Content content = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        content.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        
        WriteResult<Content, Content> result = getSubjectGenerator().getContentStore().writeContent(content);
        assertTrue("Failed to write " + content, result.written());
        
        getSubjectGenerator().getEquivalentContentStore().updateContent(content);
        
        ResolvedEquivalents<Content> resolved
            = get(getSubjectGenerator().getEquivalentContentStore().resolveIds(ImmutableList.of(Id.valueOf(1)), ImmutableSet.of(Publisher.BBC), Annotation.all()));
        
        ImmutableSet<Content> set = resolved.get(content.getId());
        assertTrue(set.isEmpty());
    }
    
    public void testResolvingIdOfUnavailableContentWhenResultingSetIsNotEmpty() throws Exception {
        Content content1 = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        content1.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        content1.setTitle("Two");
        
        Content content2 = new Item(Id.valueOf(2), Publisher.BBC);
        content2.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        content2.setTitle("Three");
        
        getSubjectGenerator().getContentStore().writeContent(content1);
        getSubjectGenerator().getContentStore().writeContent(content2);
        
        Optional<EquivalenceGraphUpdate> update
            = getSubjectGenerator().getEquivalenceGraphStore().updateEquivalences(content1.toRef(), ImmutableSet.<ResourceRef>of(content2.toRef()), 
                    ImmutableSet.of(Publisher.METABROADCAST, Publisher.BBC));
        
        getSubjectGenerator().getEquivalentContentStore().updateEquivalences(update.get());
        
        ResolvedEquivalents<Content> resolved
            = get(getSubjectGenerator().getEquivalentContentStore().resolveIds(ImmutableList.of(content2.getId()), 
                    ImmutableSet.of(Publisher.METABROADCAST), Annotation.all()));
        
        ImmutableSet<Content> set = resolved.get(content2.getId());
        assertThat(Iterables.getOnlyElement(set), is(content1));
    }
    
    public void testResolvingIdsWhereOneIsAvailableOneDoesntExist() throws Exception {
        Content content = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        content.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        
        WriteResult<Content, Content> result = getSubjectGenerator().getContentStore().writeContent(content);
        assertTrue("Failed to write " + content, result.written());
        
        getSubjectGenerator().getEquivalentContentStore().updateContent(content);
        
        ResolvedEquivalents<Content> resolved
            = get(getSubjectGenerator().getEquivalentContentStore().resolveIds(ImmutableList.of(content.getId(), Id.valueOf(4)), Publisher.all(), Annotation.all()));
        
        ImmutableSet<Content> set = resolved.get(content.getId());
        assertThat(Iterables.getOnlyElement(set), is(content));
        assertTrue(resolved.get(Id.valueOf(4)).isEmpty());
    }

    public void testDoesntResolveNotActivelyPublishedContent() throws Exception {
        Item content1 = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        content1.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        content1.setTitle("Two");
        content1.setSegmentEvents(ImmutableList.of(new SegmentEvent()));

        Content content2 = new Item(Id.valueOf(2), Publisher.BBC);
        content2.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        content2.setTitle("Three");

        getSubjectGenerator().getContentStore().writeContent(content1);
        getSubjectGenerator().getContentStore().writeContent(content2);

        Optional<EquivalenceGraphUpdate> update
                = getSubjectGenerator().getEquivalenceGraphStore().updateEquivalences(content1.toRef(), ImmutableSet.<ResourceRef>of(content2.toRef()),
                ImmutableSet.of(Publisher.METABROADCAST, Publisher.BBC));


        content2.setActivelyPublished(false);

        getSubjectGenerator().getContentStore().writeContent(content2);
        getSubjectGenerator().getEquivalentContentStore().updateEquivalences(update.get());

        ResolvedEquivalents<Content> resolved
                = get(getSubjectGenerator().getEquivalentContentStore().resolveIds(ImmutableList.of(content1.getId()), ImmutableSet.of(Publisher.METABROADCAST, Publisher.BBC), Annotation.all()));

        ImmutableSet<Content> set = resolved.get(content1.getId());
        assertThat(set, hasSize(1));
        assertThat(set, hasItems(content1));
        assertThat(Iterables.getOnlyElement(((Item) set.asList().get(0)).getSegmentEvents()).getSource(), is(Publisher.METABROADCAST));
    }


    public void testResolvesFullEquivalentSet() throws Exception {
        Item content1 = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        content1.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        content1.setTitle("Two");
        content1.setSegmentEvents(ImmutableList.of(new SegmentEvent()));

        Content content2 = new Item(Id.valueOf(2), Publisher.BBC);
        content2.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        content2.setTitle("Three");

        getSubjectGenerator().getContentStore().writeContent(content1);
        getSubjectGenerator().getContentStore().writeContent(content2);

        Optional<EquivalenceGraphUpdate> update
                = getSubjectGenerator().getEquivalenceGraphStore().updateEquivalences(content1.toRef(), ImmutableSet.<ResourceRef>of(content2.toRef()),
                ImmutableSet.of(Publisher.METABROADCAST, Publisher.BBC));

        getSubjectGenerator().getEquivalentContentStore().updateEquivalences(update.get());

        Set<Content> content = get(getSubjectGenerator().getEquivalentContentStore().resolveEquivalentSet(1L));

        assertThat(content, is(ImmutableSet.of(content1, content2)));
    }
    
}
