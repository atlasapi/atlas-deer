package org.atlasapi.content;

import javax.annotation.Nullable;

import org.atlasapi.serialization.protobuf.CommonProtos;

import com.google.common.base.Function;

public class RelatedLinkSerializer {

    public CommonProtos.RelatedLink serialize(RelatedLink relatedLink) {
        CommonProtos.RelatedLink.Builder link = CommonProtos.RelatedLink.newBuilder();
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

    public RelatedLink deserialize(CommonProtos.RelatedLink link) {
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

    public final Function<RelatedLink, CommonProtos.RelatedLink> TO_PROTO =
            new Function<RelatedLink, CommonProtos.RelatedLink>() {

                @Nullable
                @Override
                public CommonProtos.RelatedLink apply(@Nullable RelatedLink relatedLink) {
                    return serialize(relatedLink);
                }
            };

    public final Function<CommonProtos.RelatedLink, RelatedLink> FROM_PROTO =
            new Function<CommonProtos.RelatedLink, RelatedLink>() {

                @Nullable
                @Override
                public RelatedLink apply(@Nullable CommonProtos.RelatedLink msg) {
                    return deserialize(msg);
                }
            };
}
