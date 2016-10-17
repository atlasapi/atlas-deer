package org.atlasapi.entity;

import java.util.Locale;
import java.util.Objects;

import org.atlasapi.hashing.Hashable;
import org.atlasapi.meta.annotations.FieldName;

public class LocalizedTitle implements Hashable {


    private String title;
    private String type;
    private Locale locale;

    @FieldName("title")
    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    @FieldName("type")
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @FieldName("locale")
    public Locale getLocale() {
        return locale;
    }

    public void setLocale(Locale locale) {
        this.locale = locale;
    }

    public LocalizedTitle copy() {
        LocalizedTitle localizedTitleCopy = new LocalizedTitle();
        localizedTitleCopy.setTitle(this.getTitle());
        localizedTitleCopy.setType(this.getType());
        localizedTitleCopy.setLocale(this.getLocale());
        return localizedTitleCopy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LocalizedTitle)) {
            return false;
        }
        LocalizedTitle that = (LocalizedTitle) o;
        return Objects.equals(title, that.title) &&
                Objects.equals(type, that.type) &&
                Objects.equals(locale, that.locale);
    }

    @Override
    public int hashCode() {
        return Objects.hash(title, type, locale);
    }
}
