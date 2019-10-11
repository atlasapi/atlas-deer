package org.atlasapi.content.v2.model.udt;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;

import java.util.Objects;

@UDT(name = "award")
public class Award {

    @Field(name = "outcome") private String outcome;
    @Field(name = "title") private String title;
    @Field(name = "description") private String description;
    @Field(name = "year") private Integer year;

    public Award() {}

    public String getOutcome() {
        return outcome;
    }

    public void setOutcome(String outcome) {
        this.outcome = outcome;
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

    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        Award award = (Award) object;
        return Objects.equals(outcome, award.outcome) &&
                Objects.equals(title, award.title) &&
                Objects.equals(description, award.description) &&
                Objects.equals(year, award.year);
    }
}
