package org.atlasapi.content.v2.model.udt;

import java.util.Objects;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;

@UDT(name = "localizedTitle")
public class LocalizedTitle {

    @Field(name = "title") private String title;
    @Field(name = "locale") private String locale;

    public LocalizedTitle() { }

    public String getLocale() {
        return locale;
    }

    public void setLocale(String locale) {
        this.locale = locale;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public boolean equals(Object object) {
        if (this == object)
            return true;
        if (object == null || getClass() != object.getClass())
            return false;
        LocalizedTitle that = (LocalizedTitle) object;
        return Objects.equals(title, that.title) &&
                Objects.equals(locale, that.locale);
    }

    @Override
    public int hashCode() {
        return Objects.hash(title, locale);
    }

}
