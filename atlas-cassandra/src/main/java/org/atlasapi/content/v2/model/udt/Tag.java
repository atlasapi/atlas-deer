package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;

import java.util.Objects;

@UDT(name = "tag")
public class Tag {

    @Field(name = "topic") private Long topic;
    @Field(name = "publisher") private String publisher;
    @Field(name = "supervised") private Boolean supervised;
    @Field(name = "weighting") private Float weighting;
    @Field(name = "relationship") private String relationship;
    @Field(name = "offset") private Integer offset;

    public Tag() {}

    public Long getTopic() {
        return topic;
    }

    public void setTopic(Long topic) {
        this.topic = topic;
    }

    public String getPublisher() {
        return publisher;
    }

    public void setPublisher(String publisher) {
        this.publisher = publisher;
    }

    public Boolean getSupervised() {
        return supervised;
    }

    public void setSupervised(Boolean supervised) {
        this.supervised = supervised;
    }

    public Float getWeighting() {
        return weighting;
    }

    public void setWeighting(Float weighting) {
        this.weighting = weighting;
    }

    public String getRelationship() {
        return relationship;
    }

    public void setRelationship(String relationship) {
        this.relationship = relationship;
    }

    public Integer getOffset() {
        return offset;
    }

    public void setOffset(Integer offset) {
        this.offset = offset;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        Tag tag = (Tag) object;
        return Objects.equals(topic, tag.topic) &&
                Objects.equals(publisher, tag.publisher) &&
                Objects.equals(supervised, tag.supervised) &&
                Objects.equals(weighting, tag.weighting) &&
                Objects.equals(relationship, tag.relationship) &&
                Objects.equals(offset, tag.offset);
    }
}
