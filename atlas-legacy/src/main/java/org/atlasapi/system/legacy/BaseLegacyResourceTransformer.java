package org.atlasapi.system.legacy;

import java.util.Set;

import org.atlasapi.content.Image;
import org.atlasapi.content.RelatedLink;
import org.atlasapi.entity.Alias;
import org.atlasapi.media.entity.Identified;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

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


	protected Iterable<Image> transformImages(Set<org.atlasapi.media.entity.Image> images) {
		if (images == null) {
			return ImmutableList.of();
		}

		return Iterables.transform(images, new Function<org.atlasapi.media.entity.Image, Image>() {
			@Override
			public Image apply(org.atlasapi.media.entity.Image input) {
				return transformImage(input);
			}
		});
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
