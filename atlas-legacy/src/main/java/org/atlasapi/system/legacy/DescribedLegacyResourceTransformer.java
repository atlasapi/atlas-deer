package org.atlasapi.system.legacy;

import java.util.Set;

import org.atlasapi.content.Image;
import org.atlasapi.content.MediaType;
import org.atlasapi.content.RelatedLink;
import org.atlasapi.content.Specialization;
import org.atlasapi.content.Synopses;
import org.atlasapi.entity.Alias;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Topic;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;


public abstract class DescribedLegacyResourceTransformer<F extends Described, T extends org.atlasapi.content.Described>
    extends BaseLegacyResourceTransformer<F, T> {
    
    protected final Logger log = LoggerFactory.getLogger(getClass());

    @Override
    public final T apply(F input) {
        T described = createDescribed(input);
        
        setIdentifiedFields(described, input);
        
        described.addAliases(moreAliases(input));

        described.setActivelyPublished(input.isActivelyPublished());
        described.setDescription(input.getDescription());
        described.setFirstSeen(input.getFirstSeen());
        described.setGenres(input.getGenres());
        described.setImage(input.getImage());
        described.setImages(transformImages(input.getImages()));
        described.setLastFetched(input.getLastFetched());
        described.setLongDescription(input.getLongDescription());
        described.setMediaType(transformEnum(input.getMediaType(), MediaType.class));
        described.setMediumDescription(input.getMediumDescription());
        described.setPresentationChannel(input.getPresentationChannel());
        described.setPublisher(input.getPublisher());
        described.setRelatedLinks(transformRelatedLinks(input.getRelatedLinks()));
        described.setScheduleOnly(input.isScheduleOnly());
        described.setShortDescription(input.getShortDescription());
        described.setSynopses(getSynopses(input));
        described.setSpecialization(transformEnum(input.getSpecialization(), Specialization.class));
        described.setTags(input.getTags());
        if (input.getThisOrChildLastUpdated() != null) {
            described.setThisOrChildLastUpdated(input.getThisOrChildLastUpdated());
        } else {
            described.setThisOrChildLastUpdated(DateTime.now());
        }
        described.setThumbnail(input.getThumbnail());
        described.setTitle(input.getTitle());
        return described;
    }

    protected <I extends org.atlasapi.content.Identified> void setIdentifiedFields(I i, Identified input) {
        i.setAliases(transformAliases(input));
        i.setCanonicalUri(input.getCanonicalUri());
        i.setCurie(input.getCurie());
        i.setEquivalenceUpdate(input.getEquivalenceUpdate());
        if (input instanceof Content || input instanceof Topic || input.getId() != null) {
            i.setId(input.getId());
        }
        if (input.getLastUpdated() != null) {
            i.setLastUpdated(input.getLastUpdated());
        } else {
            i.setLastUpdated(DateTime.now());
        }
    }



    protected abstract T createDescribed(F input);
    
    private Synopses getSynopses(org.atlasapi.media.entity.Described input) {
        Synopses synopses = Synopses.withShortDescription(input.getShortDescription());
        synopses.setMediumDescription(input.getMediumDescription());
        synopses.setLongDescription(input.getLongDescription());
        return synopses;
    }

    protected abstract Iterable<Alias> moreAliases(F input);

}
