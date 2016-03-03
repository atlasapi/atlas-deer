package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.UDT;

@UDT(name = "tag")
public class Tag {

    private Long topic;
    private String publisher;
    private Boolean supervised;
    private Float weighting;
    private String relationship;
    private Integer offset;

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
}
