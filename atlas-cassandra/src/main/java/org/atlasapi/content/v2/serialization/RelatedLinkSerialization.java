package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.v2.model.udt.RelatedLink;

public class RelatedLinkSerialization {

    public RelatedLink serialize(org.atlasapi.content.RelatedLink rl) {
        if (rl == null) {
            return null;
        }
        RelatedLink link = new RelatedLink();

        link.setUrl(rl.getUrl());
        org.atlasapi.content.RelatedLink.LinkType type = rl.getType();
        if (type != null) {
            link.setType(type.name());
        }
        link.setSourceId(rl.getSourceId());
        link.setShortName(rl.getShortName());
        link.setTitle(rl.getTitle());
        link.setDescription(rl.getDescription());
        link.setImage(rl.getImage());
        link.setThumbnail(rl.getThumbnail());

        return link;
    }

    public org.atlasapi.content.RelatedLink deserialize(RelatedLink rl) {
        if (rl == null) {
            return null;
        }
        org.atlasapi.content.RelatedLink.LinkType type =
                org.atlasapi.content.RelatedLink.LinkType.valueOf(rl.getType());

        return org.atlasapi.content.RelatedLink.relatedLink(type, rl.getUrl())
                .withTitle(rl.getTitle())
                .withDescription(rl.getDescription())
                .withImage(rl.getImage())
                .withShortName(rl.getShortName())
                .withSourceId(rl.getSourceId())
                .withThumbnail(rl.getThumbnail())
                .build();
    }

}