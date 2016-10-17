package org.atlasapi.entity;

import java.util.Set;

import org.atlasapi.content.ContentGroup;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.meta.annotations.FieldName;

import com.metabroadcast.common.url.UrlEncoding;

import com.google.common.collect.Sets;
import org.joda.time.DateTime;

public class Person extends ContentGroup {

    public static final String BASE_URI = "http://people.atlasapi.org/%s/%s";

    private String givenName;
    private String familyName;
    private String gender;
    private DateTime birthDate;
    private String birthPlace;
    private Set<String> quotes = Sets.newHashSet();

    private String pseudoForename;
    private String pseudoSurname;
    private String additionalInfo;
    private String billing;
    private String personSource;
    private String sourceTitle;

    @FieldName("given_name")
    public String getGivenName() {
        return givenName;
    }

    public void setGivenName(String givenName) {
        this.givenName = givenName;
    }

    @FieldName("family_name")
    public String getFamilyName() {
        return familyName;
    }

    public void setFamilyName(String familyName) {
        this.familyName = familyName;
    }

    @FieldName("gender")
    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    @FieldName("birth_date")
    public DateTime getBirthDate() {
        return birthDate;
    }

    public void setBirthDate(DateTime birthDate) {
        this.birthDate = birthDate;
    }

    @FieldName("birth_place")
    public String getBirthPlace() {
        return birthPlace;
    }

    public void setBirthPlace(String birthPlace) {
        this.birthPlace = birthPlace;
    }

    @FieldName("quotes")
    public Set<String> getQuotes() {
        return quotes;
    }

    public void setQuotes(Iterable<String> quotes) {
        this.quotes = Sets.newHashSet(quotes);
    }

    public void addQuote(String quote) {
        this.quotes.add(quote);
    }

    public Person() { /* required for legacy code */ }

    public Person(String uri, String curie, Publisher publisher) {
        super(ContentGroup.Type.PERSON, uri, curie, publisher);
    }

    public String getPseudoForename() {
        return pseudoForename;
    }

    public void setPseudoForename(String pseudoForename) {
        this.pseudoForename = pseudoForename;
    }

    public String getPseudoSurname() {
        return pseudoSurname;
    }

    public void setPseudoSurname(String pseudoSurname) {
        this.pseudoSurname = pseudoSurname;
    }

    public String getAdditionalInfo() {
        return additionalInfo;
    }

    public void setAdditionalInfo(String additionalInfo) {
        this.additionalInfo = additionalInfo;
    }

    public String getBilling() {
        return billing;
    }

    public void setBilling(String billing) {
        this.billing = billing;
    }

    public String getPersonSource() {
        return personSource;
    }

    public void setPersonSource(String source) {
        this.personSource = source;
    }

    public String getSourceTitle() {
        return sourceTitle;
    }

    public void setSourceTitle(String sourceTitle) {
        this.sourceTitle = sourceTitle;
    }

    @FieldName("name")
    public String name() {
        return this.getTitle();
    }

    @FieldName("profile_links")
    public Set<String> profileLinks() {
        return this.getAliasUrls();
    }

    public Person withProfileLink(String profileLink) {
        this.addAliasUrl(profileLink);
        return this;
    }

    public Person withProfileLinks(Set<String> profileLinks) {
        this.setAliasUrls(profileLinks);
        return this;
    }

    public Person withName(String name) {
        this.setTitle(name);
        return this;
    }

    public static String formatForUri(String key) {
        return UrlEncoding.encode(key.toLowerCase().replace(' ', '_'));
    }

    @Override
    public Person copy() {
        Person copy = new Person();
        ContentGroup.copyTo(this, copy);
        copy.setGivenName(givenName);
        copy.setFamilyName(familyName);
        copy.setGender(gender);
        copy.setBirthDate(birthDate);
        copy.setBirthPlace(birthPlace);
        copy.setQuotes(quotes);
        return copy;
    }
}
