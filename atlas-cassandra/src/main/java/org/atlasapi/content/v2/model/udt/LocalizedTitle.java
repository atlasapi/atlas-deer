package org.atlasapi.content.v2.model.udt;

import java.util.Locale;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;

@UDT(name = "localizedTitle")
public class LocalizedTitle {

    @Field(name = "title") private String title;
    @Field(name = "type") private String type;
    @Field(name = "locale") private Locale locale;

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Locale getLocale() {
        return locale;
    }

    public void setLocale(Locale locale) {
        this.locale = locale;
    }
}
