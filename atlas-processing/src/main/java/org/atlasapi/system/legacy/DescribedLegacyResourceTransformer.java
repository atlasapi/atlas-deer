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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
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
        described.setThisOrChildLastUpdated(input.getThisOrChildLastUpdated());
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
        i.setLastUpdated(input.getLastUpdated());
    }

    protected abstract T createDescribed(F input);
    
    protected <E extends Enum<E>> E transformEnum(Enum<?> from, Class<E> to) {
        if (from == null) {
            return null;
        }
        try {
            return Enum.valueOf(to, from.name());
        } catch (IllegalArgumentException e) {
            log.warn("{} missing constant {}", to, from);
            return null;
        }
    }

    private Synopses getSynopses(org.atlasapi.media.entity.Described input) {
        Synopses synopses = Synopses.withShortDescription(input.getShortDescription());
        synopses.setMediumDescription(input.getMediumDescription());
        synopses.setLongDescription(input.getLongDescription());
        return synopses;
    }

    private Iterable<RelatedLink> transformRelatedLinks(
            Set<org.atlasapi.media.entity.RelatedLink> relatedLinks) {
        return Iterables.transform(relatedLinks,
            new Function<org.atlasapi.media.entity.RelatedLink, RelatedLink>() {
                @Override
                public RelatedLink apply(org.atlasapi.media.entity.RelatedLink input) {
                    RelatedLink.LinkType type = transformEnum(input.getType(), RelatedLink.LinkType.class);
                    return RelatedLink.relatedLink(type, input.getUrl())
                            .withDescription(input.getDescription())
                            .withImage(input.getImage())
                            .withShortName(input.getShortName())
                            .withSourceId(input.getSourceId())
                            .withThumbnail(input.getThumbnail())
                            .withTitle(input.getTitle())
                            .build();
                }
            }
        );
    }

    private Iterable<Image> transformImages(Set<org.atlasapi.media.entity.Image> images) {
        return Iterables.transform(images, new Function<org.atlasapi.media.entity.Image, Image>() {
            @Override
            public Image apply(org.atlasapi.media.entity.Image input) {
                Image image = new Image(input.getCanonicalUri());
                image.setType(transformEnum(input.getType(), Image.Type.class));
                image.setColor(transformEnum(input.getColor(), Image.Color.class));
                image.setTheme(transformEnum(input.getTheme(), Image.Theme.class));
                image.setHeight(input.getHeight());
                image.setWidth(input.getWidth());
                image.setAspectRatio(transformEnum(input.getAspectRatio(), Image.AspectRatio.class));
                image.setMimeType(input.getMimeType());
                image.setAvailabilityStart(input.getAvailabilityStart());
                image.setAvailabilityEnd(input.getAvailabilityEnd());
                return image;
            }
        });
    }

    protected ImmutableSet<Alias> transformAliases(Identified input) {
        ImmutableSet.Builder<Alias> aliases = ImmutableSet.builder();
        aliases.addAll(transformAliases(input.getAliases()));
        aliases.addAll(Collections2.transform(input.getAliasUrls(),
            new Function<String, Alias>() {
                @Override
                public Alias apply(String input) {
                    return new Alias(Alias.URI_NAMESPACE, input);
                }
            }
        ));
        return aliases.build();
    }

    protected abstract Iterable<Alias> moreAliases(F input);

    private Set<? extends Alias> transformAliases(Set<org.atlasapi.media.entity.Alias> aliases) {
        return ImmutableSet.copyOf(Collections2.transform(aliases,
            new Function<org.atlasapi.media.entity.Alias, Alias>() {
                @Override
                public Alias apply(org.atlasapi.media.entity.Alias input) {
                    return new Alias(input.getNamespace(), input.getValue());
                }
            }
        ));
    }
}
