package org.atlasapi.content.v2.model.udt;

import java.util.List;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;

@UDT(name = "review")
public class Review {

    @Field(name = "locale") private String locale;
    @Field(name = "review") private String review;
    @Field(name = "type") private String type;
    @Field(name = "author") private Author author;

    public String getLocale() {
        return locale;
    }

    public void setLocale(String locale) {
        this.locale = locale;
    }

    public String getReview() {
        return review;
    }

    public void setReview(String review) {
        this.review = review;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Author getAuthor() {
        return author;
    }

    public void setPeople(Author author) {
        this.author = author;
    }
}
