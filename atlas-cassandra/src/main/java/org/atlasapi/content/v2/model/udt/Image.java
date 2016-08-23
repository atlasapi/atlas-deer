package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;
import org.joda.time.Instant;

@UDT(name = "image")
public class Image {

    @Field(name = "uri") private String uri;
    @Field(name = "type") private String type;
    @Field(name = "color") private String color;
    @Field(name = "theme") private String theme;
    @Field(name = "height") private Integer height;
    @Field(name = "width") private Integer width;
    @Field(name = "aspect") private String aspectRatio;
    @Field(name = "mime") private String mimeType;
    @Field(name = "avail_start") private Instant availabilityStart;
    @Field(name = "avail_end") private Instant availabilityEnd;
    @Field(name = "has_title_art") private Boolean hasTitleArt;
    @Field(name = "src") private String source;

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getColor() {
        return color;
    }

    public void setColor(String color) {
        this.color = color;
    }

    public String getTheme() {
        return theme;
    }

    public void setTheme(String theme) {
        this.theme = theme;
    }

    public Integer getHeight() {
        return height;
    }

    public void setHeight(Integer height) {
        this.height = height;
    }

    public Integer getWidth() {
        return width;
    }

    public void setWidth(Integer width) {
        this.width = width;
    }

    public String getAspectRatio() {
        return aspectRatio;
    }

    public void setAspectRatio(String aspectRatio) {
        this.aspectRatio = aspectRatio;
    }

    public String getMimeType() {
        return mimeType;
    }

    public void setMimeType(String mimeType) {
        this.mimeType = mimeType;
    }

    public Instant getAvailabilityStart() {
        return availabilityStart;
    }

    public void setAvailabilityStart(Instant availabilityStart) {
        this.availabilityStart = availabilityStart;
    }

    public Instant getAvailabilityEnd() {
        return availabilityEnd;
    }

    public void setAvailabilityEnd(Instant availabilityEnd) {
        this.availabilityEnd = availabilityEnd;
    }

    public Boolean getHasTitleArt() {
        return hasTitleArt;
    }

    public void setHasTitleArt(Boolean hasTitleArt) {
        this.hasTitleArt = hasTitleArt;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }
}
