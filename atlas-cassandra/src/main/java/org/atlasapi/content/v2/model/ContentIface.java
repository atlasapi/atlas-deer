package org.atlasapi.content.v2.model;

import java.util.List;
import java.util.Set;

import org.atlasapi.content.v2.model.udt.Certificate;
import org.atlasapi.content.v2.model.udt.ContentGroupRef;
import org.atlasapi.content.v2.model.udt.CrewMember;
import org.atlasapi.content.v2.model.udt.KeyPhrase;
import org.atlasapi.content.v2.model.udt.Ref;
import org.atlasapi.content.v2.model.udt.Tag;

import com.metabroadcast.common.intl.Country;

public interface ContentIface extends Described {

    Set<KeyPhrase> getKeyPhrases();

    void setKeyPhrases(Set<KeyPhrase> keyPhrases);

    List<Tag> getTags();

    void setTags(List<Tag> tags);

    Set<ContentGroupRef> getContentGroupRefs();

    void setContentGroupRefs(Set<ContentGroupRef> contentGroupRefs);

    List<CrewMember> getPeople();

    void setPeople(List<CrewMember> people);

    Set<String> getLanguages();

    void setLanguages(Set<String> languages);

    Set<Certificate> getCertificates();

    void setCertificates(Set<Certificate> certificates);

    Integer getYear();

    void setYear(Integer year);

    Boolean getGenericDescription();

    void setGenericDescription(Boolean genericDescription);

    Set<Ref> getEventRefs();

    void setEventRefs(Set<Ref> eventRefs);

    Clip.Wrapper getClips();

    void setClips(Clip.Wrapper clips);

    Encoding.Wrapper getEncodings();

    void setEncodings(Encoding.Wrapper encodings);

    Set<String> getCountriesOfOrigin();

    void setCountriesOfOrigin(Set<String> countriesOfOrigin);
}
