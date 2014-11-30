package org.atlasapi.content;

import javax.annotation.Nullable;

import org.atlasapi.serialization.protobuf.ContentProtos;

import com.google.common.base.Function;

public class RelatedLinkSerializer {

    public ContentProtos.RelatedLink serialize(RelatedLink relatedLink) {
        ContentProtos.RelatedLink.Builder link = ContentProtos.RelatedLink.newBuilder();
        link.setUrl(relatedLink.getUrl());
        link.setType(relatedLink.getType().toString());
        if (relatedLink.getSourceId() != null) {
            link.setSourceId(relatedLink.getSourceId());
        }
        if (relatedLink.getShortName() != null) {
            link.setShortName(relatedLink.getShortName());
        }
        if (relatedLink.getTitle() != null) {
            link.setTitle(relatedLink.getTitle());
        }
        if (relatedLink.getDescription() != null) {
            link.setDescription(relatedLink.getDescription());
        }
        if (relatedLink.getImage() != null) {
            link.setImage(relatedLink.getImage());
        }
        if (relatedLink.getThumbnail() != null) {
            link.setThumbnail(relatedLink.getThumbnail());
        }
        return link.build();
    }

    public RelatedLink deserialize(ContentProtos.RelatedLink link) {
        RelatedLink.LinkType type = RelatedLink.LinkType.valueOf(link.getType());
        RelatedLink relatedLink = RelatedLink.relatedLink(type, link.getUrl())
                .withSourceId(link.hasSourceId() ? link.getSourceId() : null)
                .withShortName(link.hasShortName() ? link.getShortName() : null)
                .withTitle(link.hasTitle() ? link.getTitle() : null)
                .withDescription(link.hasDescription() ? link.getDescription() : null)
                .withImage(link.hasImage() ? link.getImage() : null)
                .withThumbnail(link.hasThumbnail() ? link.getThumbnail() : null)
                .build();
        return relatedLink;
    }

    public final Function<RelatedLink, ContentProtos.RelatedLink> TO_PROTO =
            new Function<RelatedLink, ContentProtos.RelatedLink>() {
                @Nullable
                @Override
                public ContentProtos.RelatedLink apply(@Nullable RelatedLink relatedLink) {
                    return serialize(relatedLink);
                }
            };

    public final Function<ContentProtos.RelatedLink, RelatedLink> FROM_PROTO =
            new Function<ContentProtos.RelatedLink, RelatedLink>() {
                @Nullable
                @Override
                public RelatedLink apply(@Nullable ContentProtos.RelatedLink msg) {
                    return deserialize(msg);
                }
            };
}
