package org.atlasapi.output;

import static com.metabroadcast.common.time.DateTimeZones.UTC;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.atlasapi.application.ApplicationSources;
import org.atlasapi.application.SourceReadEntry;
import org.atlasapi.application.SourceStatus;
import org.atlasapi.content.BlackoutRestriction;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Item;
import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.DateTime;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public class BroadcastMergingTest {

    private final OutputContentMerger executor = new OutputContentMerger();
    private final ApplicationSources sources = ApplicationSources.defaults()
            .copy().withPrecedence(true)
            .withReadableSources(ImmutableList.of(
                    new SourceReadEntry(Publisher.BBC, SourceStatus.AVAILABLE_ENABLED),
                    new SourceReadEntry(Publisher.FACEBOOK, SourceStatus.AVAILABLE_ENABLED)
             ))
            .build();
    
    @Test
    public void testBroadcastMergingNoBroadcasts() {
        Item chosenItem = new Item();
        chosenItem.setId(1L);
        chosenItem.setPublisher(Publisher.BBC);
        chosenItem.setCanonicalUri("chosenItem");

        Item notChosenItem = new Item();
        notChosenItem.setId(2L);
        notChosenItem.setPublisher(Publisher.FACEBOOK);
        notChosenItem.setCanonicalUri("notChosenItem");
        
        chosenItem.addEquivalentTo(notChosenItem);
        notChosenItem.addEquivalentTo(chosenItem);
        
        executor.merge(chosenItem, ImmutableList.of(notChosenItem), sources);
        
        assertTrue(notChosenItem.getBroadcasts().isEmpty());
    }
    
    @Test
    public void testBroadcastMergingNonMatchingBroadcasts() {
        Item chosenItem = new Item();
        chosenItem.setId(1L);
        chosenItem.setPublisher(Publisher.BBC);
        chosenItem.setCanonicalUri("chosenItem");
        chosenItem.addBroadcast(new Broadcast(Id.valueOf(2), new DateTime(2012,1,1,0,0,0,UTC), new DateTime(2012,1,1,0,0,0,UTC)));

        Item notChosenItem = new Item();
        notChosenItem.setId(2L);
        notChosenItem.setPublisher(Publisher.FACEBOOK);
        notChosenItem.setCanonicalUri("notChosenItem");
        // different broadcast channel
        notChosenItem.addBroadcast(new Broadcast(Id.valueOf(1), new DateTime(2012,1,1,0,0,0,UTC), new DateTime(2012,1,1,0,0,0,UTC)));
        // different start time
        notChosenItem.addBroadcast(new Broadcast(Id.valueOf(2), new DateTime(2012,1,4,0,0,0,UTC), new DateTime(2012,1,4,0,0,0,UTC)));
        
        chosenItem.addEquivalentTo(notChosenItem);
        notChosenItem.addEquivalentTo(chosenItem);
        
        executor.merge(chosenItem, ImmutableList.of(notChosenItem), sources);
        
        assertTrue(chosenItem.getBroadcasts().size() == 1);
    }
    
    @Test
    public void testBroadcastMergingMatchingBroadcasts() {
        Item chosenItem = new Item();
        chosenItem.setId(1L);
        chosenItem.setPublisher(Publisher.BBC);
        chosenItem.setCanonicalUri("chosenItem");
        Broadcast chosenBroadcast = new Broadcast(Id.valueOf(2), new DateTime(2012,1,1,0,0,0,UTC), new DateTime(2012,1,1,0,0,0,UTC));
        chosenBroadcast.addAliasUrl("chosenBroadcast");
        chosenBroadcast.addAlias(new Alias("chosenNamspace", "chosenValue"));
        chosenBroadcast.setSubtitled(true);
        chosenItem.addBroadcast(chosenBroadcast);

        Item notChosenItem = new Item();
        notChosenItem.setId(2L);
        notChosenItem.setCanonicalUri("notChosenItem");
        notChosenItem.setPublisher(Publisher.FACEBOOK);
        Broadcast broadcast = new Broadcast(Id.valueOf(2), new DateTime(2012,1,1,0,0,0,UTC), new DateTime(2012,1,1,0,0,0,UTC));
        broadcast.addAliasUrl("non-chosen alias");
        broadcast.addAlias(new Alias("notChosenNamespace", "notChosenValue"));
        broadcast.setAudioDescribed(true);
        broadcast.setHighDefinition(false);
        broadcast.setSurround(false);
        broadcast.setSubtitled(false);
        broadcast.setBlackoutRestriction(new BlackoutRestriction(true));
        notChosenItem.addBroadcast(broadcast);
        
        chosenItem.addEquivalentTo(notChosenItem);
        notChosenItem.addEquivalentTo(chosenItem);
        
        executor.merge(chosenItem, ImmutableList.of(notChosenItem), sources);
        
        // ensure that the broadcast matched, 
        // and the fields on the non-chosen broadcast 
        // are merged only when the original broadcast's fields are null
        Broadcast mergedBroadcast = Iterables.getOnlyElement(chosenItem.getBroadcasts());
        assertTrue(mergedBroadcast.getAudioDescribed());
        assertFalse(mergedBroadcast.getHighDefinition());
        assertFalse(mergedBroadcast.getSurround());
        assertTrue(mergedBroadcast.getSubtitled());
        assertTrue(mergedBroadcast.getAliases().size() == 2);
        assertTrue(mergedBroadcast.getBlackoutRestriction().isPresent());
    }
    
    @Test
    public void testBroadcastMergingMatchingBroadcastsWithPrecedence() {
        
        ApplicationSources sources = ApplicationSources.defaults()
                .copy().withPrecedence(true)
                .withReadableSources(ImmutableList.of(
                        new SourceReadEntry(Publisher.METABROADCAST, SourceStatus.AVAILABLE_ENABLED),
                        new SourceReadEntry(Publisher.PA, SourceStatus.AVAILABLE_ENABLED),
                        new SourceReadEntry(Publisher.BBC, SourceStatus.AVAILABLE_ENABLED),
                        new SourceReadEntry(Publisher.FACEBOOK, SourceStatus.AVAILABLE_ENABLED)
                 ))
                .build();
        
        Item chosenItemWithoutBroadcasts = new Item();
        chosenItemWithoutBroadcasts.setId(1L);
        chosenItemWithoutBroadcasts.setPublisher(Publisher.METABROADCAST);
        
        Item notChosenFirstBbcItem = new Item();
        notChosenFirstBbcItem.setId(2L);
        notChosenFirstBbcItem.setCanonicalUri("chosenItem");
        notChosenFirstBbcItem.setPublisher(Publisher.PA);
        Broadcast mostPrecedentBroadcast = new Broadcast(Id.valueOf(2), new DateTime(2012,1,1,0,0,0,UTC), new DateTime(2012,1,1,0,0,0,UTC));
        mostPrecedentBroadcast.addAliasUrl("chosenBroadcast");
        mostPrecedentBroadcast.addAlias(new Alias("chosenNamspace", "chosenValue"));
        mostPrecedentBroadcast.setSubtitled(true);
        notChosenFirstBbcItem.addBroadcast(mostPrecedentBroadcast);

        Item notChosenBbcItem = new Item();
        notChosenBbcItem.setId(3L);
        notChosenBbcItem.setCanonicalUri("notChosenItem");
        notChosenBbcItem.setPublisher(Publisher.BBC);
        Broadcast broadcast = new Broadcast(Id.valueOf(2), new DateTime(2012,1,1,0,0,0,UTC), new DateTime(2012,1,1,0,0,0,UTC));
        broadcast.addAliasUrl("non-chosen alias");
        broadcast.addAlias(new Alias("notChosenNamespace", "notChosenValue"));
        broadcast.setAudioDescribed(true);
        broadcast.setHighDefinition(true);
        broadcast.setSubtitled(false);
        notChosenBbcItem.addBroadcast(broadcast);
        
        Item notChosenFbItem = new Item();
        notChosenFbItem.setId(4L);
        notChosenFbItem.setCanonicalUri("notChosenItem");
        notChosenFbItem.setPublisher(Publisher.FACEBOOK);
        broadcast = new Broadcast(Id.valueOf(2), new DateTime(2012,1,1,0,0,0,UTC), new DateTime(2012,1,1,0,0,0,UTC));
        broadcast.addAliasUrl("non-chosen alias");
        broadcast.addAlias(new Alias("notChosenFBNamespace", "notChosenFBValue"));
        broadcast.setAudioDescribed(true);
        broadcast.setHighDefinition(false);
        broadcast.setSurround(false);
        broadcast.setSubtitled(false);
        notChosenFbItem.addBroadcast(broadcast);
        
        chosenItemWithoutBroadcasts.addEquivalentTo(notChosenFirstBbcItem);
        notChosenFirstBbcItem.addEquivalentTo(notChosenBbcItem);
        notChosenFirstBbcItem.addEquivalentTo(notChosenFbItem);
        notChosenBbcItem.addEquivalentTo(notChosenFirstBbcItem);
        notChosenBbcItem.addEquivalentTo(notChosenFbItem);
        notChosenFbItem.addEquivalentTo(notChosenFirstBbcItem);
        notChosenFbItem.addEquivalentTo(notChosenBbcItem);

        executor.merge(chosenItemWithoutBroadcasts, ImmutableList.of(notChosenFirstBbcItem, notChosenBbcItem, notChosenFbItem), sources);
        
        // ensure that the broadcast matched, 
        // and the fields on the non-chosen broadcast 
        // are merged only when the original broadcast's fields are null
        // and that the most precedent broadcast's values are used
        Broadcast mergedBroadcast = Iterables.getOnlyElement(chosenItemWithoutBroadcasts.getBroadcasts());
        assertTrue(mergedBroadcast.getAudioDescribed());
        assertTrue(mergedBroadcast.getHighDefinition());
        assertFalse(mergedBroadcast.getSurround());
        assertTrue(mergedBroadcast.getSubtitled());
        assertTrue(mergedBroadcast.getAliases().size() == 3);
    }
}
