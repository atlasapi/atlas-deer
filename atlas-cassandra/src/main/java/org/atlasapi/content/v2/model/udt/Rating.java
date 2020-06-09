package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;

import java.util.Objects;

@UDT(name = "rating")
public class Rating {

    @Field(name = "value") private float value;
    @Field(name = "type") private String type;
    @Field(name = "publisher") private String publisher;
    @Field(name = "number_of_votes") private Long numberOfVotes;

    public Rating() {}

    public float getValue() {
        return value;
    }

    public void setValue(float value) {
        this.value = value;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getPublisher() {
        return publisher;
    }

    public void setPublisher(String publisher) {
        this.publisher = publisher;
    }

    public Long getNumberOfVotes() {
        return numberOfVotes;
    }

    public void setNumberOfVotes(long numberOfVotes) {
        this.numberOfVotes = numberOfVotes;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        Rating rating = (Rating) object;
        return Float.compare(rating.value, value) == 0 &&
                Objects.equals(type, rating.type) &&
                Objects.equals(publisher, rating.publisher) &&
                Objects.equals(numberOfVotes, rating.numberOfVotes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, type, publisher, numberOfVotes);
    }
}
