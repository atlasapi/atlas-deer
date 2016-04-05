package org.atlasapi.entity;

import java.util.Objects;

import org.atlasapi.meta.annotations.FieldName;

public class Award {

    private String outcome;
    private String title;
    private String description;
    private Integer year;

    @FieldName("outcome")
    public String getOutcome() {
        return outcome;
    }

    public void setOutcome(String outcome) {
        this.outcome = outcome;
    }

    @FieldName("description")
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @FieldName("title")
    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    @FieldName("year")
    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    public Award copy() {
        Award copy = new Award();
        copy.setOutcome(outcome);
        copy.setTitle(title);
        copy.setDescription(description);
        copy.setYear(year);
        return copy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Award award = (Award) o;
        return Objects.equals(outcome, award.outcome) &&
                Objects.equals(title, award.title) &&
                Objects.equals(description, award.description) &&
                Objects.equals(year, award.year);
    }

    @Override public int hashCode() {
        return Objects.hash(outcome, title, description, year);
    }
}
