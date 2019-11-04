package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;
import org.atlasapi.util.NullOrEmptyEquality;

import java.util.Objects;
import java.util.Set;

@UDT(name = "itemsummary")
public class ItemSummary {

    @Field(name = "title") private String title;
    @Field(name = "descr") private String description;
    @Field(name = "image") private String image;
    @Field(name = "release_year") private Integer releaseYear;
    @Field(name = "certs") private Set<Certificate> certificate;
    @Field(name = "type") private String type;
    @Field(name = "episode_nr") private Integer episodeNumber;

    public ItemSummary() {}

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

    public Integer getReleaseYear() {
        return releaseYear;
    }

    public void setReleaseYear(Integer releaseYear) {
        this.releaseYear = releaseYear;
    }

    public Set<Certificate> getCertificate() {
        return certificate;
    }

    public void setCertificate(Set<Certificate> certificate) {
        this.certificate = certificate;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Integer getEpisodeNumber() {
        return episodeNumber;
    }

    public void setEpisodeNumber(Integer episodeNumber) {
        this.episodeNumber = episodeNumber;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        ItemSummary that = (ItemSummary) object;
        return Objects.equals(title, that.title) &&
                Objects.equals(description, that.description) &&
                Objects.equals(image, that.image) &&
                Objects.equals(releaseYear, that.releaseYear) &&
                NullOrEmptyEquality.equals(certificate, that.certificate) &&
                Objects.equals(type, that.type) &&
                Objects.equals(episodeNumber, that.episodeNumber);
    }

    @Override
    public int hashCode() {
        return NullOrEmptyEquality.hash(title, description, image, releaseYear, certificate, type, episodeNumber);
    }
}
