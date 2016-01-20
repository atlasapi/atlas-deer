package org.atlasapi.system.legacy;

import java.util.Set;
import java.util.stream.Collectors;

import org.atlasapi.content.Image;
import org.atlasapi.content.RelatedLink;
import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.EquivalenceRef;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.util.ImmutableCollectors;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseLegacyResourceTransformer<F, T extends org.atlasapi.entity.Identified> implements
		LegacyResourceTransformer<F, T> {

	protected final Logger log = LoggerFactory.getLogger(getClass());

	@Override
	public final Iterable<T> transform(Iterable<F> legacy) {
		return Iterables.transform(legacy, this);
	}

	protected static <E extends Enum<E>> E transformEnum(Enum<?> from, Class<E> to) {
		if (from == null) {
			return null;
		}
		try {
			return Enum.valueOf(to, from.name());
		} catch (IllegalArgumentException e) {
			return null;
		}
	}

	protected Iterable<RelatedLink> transformRelatedLinks(
			Set<org.atlasapi.media.entity.RelatedLink> relatedLinks) {
		return Iterables.transform(
                relatedLinks,
                input -> {
                    RelatedLink.LinkType type = transformEnum(
                            input.getType(), RelatedLink.LinkType.class
                    );
                    return RelatedLink.relatedLink(type, input.getUrl())
                            .withDescription(input.getDescription())
                            .withImage(input.getImage())
                            .withShortName(input.getShortName())
                            .withSourceId(input.getSourceId())
                            .withThumbnail(input.getThumbnail())
                            .withTitle(input.getTitle())
                            .build();
                }
        );
	}

	protected Iterable<Image> transformImages(Set<org.atlasapi.media.entity.Image> images) {
		if (images == null) {
            return ImmutableList.of();
		}

        return images.stream()
                .filter(image -> image.getCanonicalUri() != null)
                .map(this::transformImage)
                .collect(ImmutableCollectors.toSet());
	}

	protected Image transformImage(org.atlasapi.media.entity.Image input) {
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
		image.setHasTitleArt(input.hasTitleArt());
		return image;
	}


	protected ImmutableSet<Alias> transformAliases(Identified input) {
		ImmutableSet.Builder<Alias> aliases = ImmutableSet.builder();
		aliases.addAll(transformAliases(input.getAliases()));
		aliases.addAll(Collections2.transform(
                input.getAliasUrls(), aliasUrl -> new Alias(Alias.URI_NAMESPACE, aliasUrl)
        ));
		return aliases.build();
	}

	private Set<? extends Alias> transformAliases(Set<org.atlasapi.media.entity.Alias> aliases) {
		return ImmutableSet.copyOf(Collections2.transform(
                aliases, input -> new Alias(input.getNamespace(), input.getValue())
        ));
	}

	protected void addIdentified(Identified source, org.atlasapi.entity.Identified target) {
		target.setId(Id.valueOf(source.getId()));
		target.setCanonicalUri(source.getCanonicalUri());
		target.setCurie(source.getCurie());
		target.setAliasUrls(source.getAliasUrls());
		target.setAliases(source.getAliases().stream()
				.map(alias -> new Alias(alias.getNamespace(), alias.getValue()))
				.collect(Collectors.toList()));
		target.setEquivalentTo(source.getEquivalentTo().stream()
				.map(ref -> new EquivalenceRef(Id.valueOf(ref.id()), ref.publisher()))
				.collect(Collectors.toSet()));
		target.setLastUpdated(source.getLastUpdated());
		target.setEquivalenceUpdate(source.getEquivalenceUpdate());
	}
}
