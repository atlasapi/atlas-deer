package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.v2.model.udt.Image;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.media.MimeType;

import org.joda.time.Instant;

import static org.atlasapi.content.v2.serialization.DateTimeUtils.toDateTime;

public class ImageSerialization {

    public Image serialize(org.atlasapi.content.Image img) {
        if (img == null) {
            return null;
        }
        Image image = new Image();

        image.setUri(img.getCanonicalUri());

        org.atlasapi.content.Image.Type type = img.getType();
        if (type != null) {
            image.setType(type.name());
        }

        org.atlasapi.content.Image.Color color = img.getColor();
        if (color != null) {
            image.setColor(color.name());
        }

        org.atlasapi.content.Image.Theme theme = img.getTheme();
        if (theme != null) {
            image.setTheme(theme.name());
        }

        image.setHeight(img.getHeight());
        image.setWidth(img.getWidth());

        org.atlasapi.content.Image.AspectRatio aspectRatio = img.getAspectRatio();
        if (aspectRatio != null) {
            image.setAspectRatio(aspectRatio.name());
        }

        MimeType mimeType = img.getMimeType();
        if (mimeType != null) {
            image.setMimeType(mimeType.name());
        }

        image.setAvailabilityStart(DateTimeUtils.toInstant(img.getAvailabilityStart()));
        image.setAvailabilityEnd(DateTimeUtils.toInstant(img.getAvailabilityEnd()));
        image.setHasTitleArt(img.hasTitleArt());

        Publisher source = img.getSource();
        if (source != null) {
            image.setSource(source.key());
        }

        return image;
    }

    public org.atlasapi.content.Image deserialize(Image img) {
        if (img == null) {
            return null;
        }

        org.atlasapi.content.Image newImg = new org.atlasapi.content.Image(img.getUri());

        String type = img.getType();
        if (type != null) {
            newImg.setType(org.atlasapi.content.Image.Type.valueOf(type));
        }

        String color = img.getColor();
        if (color != null) {
            newImg.setColor(org.atlasapi.content.Image.Color.valueOf(color));
        }

        String theme = img.getTheme();
        if (theme != null) {
            newImg.setTheme(org.atlasapi.content.Image.Theme.valueOf(theme));
        }

        newImg.setHeight(img.getHeight());
        newImg.setWidth(img.getWidth());

        String aspectRatio = img.getAspectRatio();
        if (aspectRatio != null) {
            newImg.setAspectRatio(org.atlasapi.content.Image.AspectRatio.valueOf(aspectRatio));
        }

        String mimeType = img.getMimeType();
        if (mimeType != null) {
            newImg.setMimeType(MimeType.valueOf(mimeType));
        }

        Instant availabilityStart = img.getAvailabilityStart();
        if (availabilityStart != null) {
            newImg.setAvailabilityStart(toDateTime(availabilityStart));
        }

        Instant availabilityEnd = img.getAvailabilityEnd();
        if (availabilityEnd != null) {
            newImg.setAvailabilityEnd(toDateTime(availabilityEnd));
        }

        newImg.setHasTitleArt(img.getHasTitleArt());

        String source = img.getSource();
        if (source != null) {
            newImg.setSource(Publisher.fromKey(source).requireValue());
        }

        return newImg;
    }

}