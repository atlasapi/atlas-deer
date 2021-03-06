package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;

import java.util.Objects;

@UDT(name = "relatedlink")
public class RelatedLink {

    @Field(name = "url") private String url;
    @Field(name = "type") private String type;
    @Field(name = "src_id") private String sourceId;
    @Field(name = "short_name") private String shortName;
    @Field(name = "title") private String title;
    @Field(name = "descr") private String description;
    @Field(name = "image") private String image;
    @Field(name = "thumbnail") private String thumbnail;

    public RelatedLink() {}

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public String getShortName() {
        return shortName;
    }

    public void setShortName(String shortName) {
        this.shortName = shortName;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public String getThumbnail() {
        return thumbnail;
    }

    public void setThumbnail(String thumbnail) {
        this.thumbnail = thumbnail;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        RelatedLink that = (RelatedLink) object;
        return Objects.equals(url, that.url) &&
                Objects.equals(type, that.type) &&
                Objects.equals(sourceId, that.sourceId) &&
                Objects.equals(shortName, that.shortName) &&
                Objects.equals(title, that.title) &&
                Objects.equals(description, that.description) &&
                Objects.equals(image, that.image) &&
                Objects.equals(thumbnail, that.thumbnail);
    }

    @Override
    public int hashCode() {
        return Objects.hash(url, type, sourceId, shortName, title, description, image, thumbnail);
    }
}
