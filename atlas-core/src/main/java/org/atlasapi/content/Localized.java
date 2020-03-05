package org.atlasapi.content;

import java.util.Locale;

import javax.annotation.Nullable;

import org.atlasapi.hashing.Hashable;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Strings;

public abstract class Localized implements Hashable {

    private static final String UNDEFINED_LANGUAGE = "und";

    private final static Function<Localized, String> TO_LANGUAGE_TAG = new Function<Localized, String>() {
        @Override
        @Nullable
        public String apply(Localized localized) {
            return localized.locale != null ? localized.locale.toLanguageTag() : null;
        }
    };

    private Locale locale;

    /**Constructs & sets the Locale of this object based on a language code, a region code, or both.
     *
     * @param languageCode  - preferably a 2 character language code (ISO 639).
     * @param regionCode    - preferably a 2 character region code (ISO 3166).
     *
     * @see java.util.Locale for more info on the codes.
     */
    public void setLocale(@Nullable String languageCode, @Nullable String regionCode) {
        Locale locale = null;

        if (!Strings.isNullOrEmpty(languageCode) && !Strings.isNullOrEmpty(regionCode)) {
            locale = Locale.forLanguageTag(String.join("/", languageCode, regionCode));
        }
        else if (!Strings.isNullOrEmpty(languageCode)) {
            locale = Locale.forLanguageTag(languageCode);   // undefined region is assumed by Locale
        }
        else if (!Strings.isNullOrEmpty(regionCode)) {
            locale = Locale.forLanguageTag(String.join("/", UNDEFINED_LANGUAGE, regionCode));
        }

        this.locale = locale;
    }

    public void setLocale(Locale locale) {
        this.locale = locale;
    }

    public Locale getLocale() {
        return locale;
    }

    public String getLanguageTag() {
        return TO_LANGUAGE_TAG.apply(this);
    }

    protected static void copyTo(Localized from, Localized to) {
        to.locale = from.locale;
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }

        if (that == null || !(that instanceof Localized)) {
            return false;
        }

        Localized thatLocalized = (Localized) that;

        return Objects.equal(this.locale, thatLocalized.locale);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(locale);
    }

    public abstract Localized copy();

}
