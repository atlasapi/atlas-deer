package org.atlasapi.content.v2.serialization;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.atlasapi.content.Content;
import org.atlasapi.content.v2.model.ContentIface;
import org.atlasapi.content.v2.model.udt.CrewMember;
import org.atlasapi.content.v2.model.udt.KeyPhrase;
import org.atlasapi.content.v2.model.udt.Ref;
import org.atlasapi.content.v2.model.udt.Tag;

public class ContentSetter {

    private final KeyPhraseSerialization keyPhrase = new KeyPhraseSerialization();
    private final ContentGroupRefSerialization contentGroupRef = new ContentGroupRefSerialization();
    private final EventRefSerialization eventRef = new EventRefSerialization();
    private final DescribedSetter describedSetter = new DescribedSetter();
    private final CrewMemberSerialization crewMember = new CrewMemberSerialization();
    private final CertificateSerialization certificate = new CertificateSerialization();
    private final TagSerialization tag = new TagSerialization();

    public void serialize(ContentIface internal, Content content) {
        describedSetter.serialize(internal, content);

        internal.setKeyPhrases(content.getKeyPhrases().stream()
                .map(keyPhrase::serialize)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));

        internal.setTags(content.getTags().stream()
                .map(tag::serialize)
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));

        internal.setContentGroupRefs(content.getContentGroupRefs().stream()
                .map(contentGroupRef::serialize)
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));

        internal.setPeople(content.people().stream()
                .map(crewMember::serialize)
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));

        internal.setLanguages(content.getLanguages());

        internal.setCertificates(content.getCertificates().stream()
                .map(certificate::serialize)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));

        internal.setYear(content.getYear());

        internal.setGenericDescription(content.isGenericDescription());
        internal.setEventRefs(content.getEventRefs().stream()
                .map(eventRef::serialize)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));
    }

    public void deserialize(Content content, ContentIface internal) {
        describedSetter.deserialize(content, internal);

        Set<KeyPhrase> keyPhrases = internal.getKeyPhrases();
        if (keyPhrases != null) {
            content.setKeyPhrases(keyPhrases.stream()
                    .map(keyPhrase::deserialize)
                    .collect(Collectors.toList()));
        }

        List<Tag> tags = internal.getTags();
        if (tags != null) {
            content.setTags(tags.stream()
                    .map(tag::deserialize)
                    .collect(Collectors.toList()));
        }

        List<org.atlasapi.content.v2.model.udt.ContentGroupRef> contentGroupRefs =
                internal.getContentGroupRefs();
        if (contentGroupRefs != null) {
            content.setContentGroupRefs(contentGroupRefs.stream()
                    .map(contentGroupRef::deserialize)
                    .collect(Collectors.toList()));
        }

        List<CrewMember> people = internal.getPeople();
        if (people != null) {
            content.setPeople(people.stream()
                    .map(crewMember::deserialize)
                    .collect(Collectors.toList()));
        }

        Set<String> languages = internal.getLanguages();
        if (languages != null) {
            content.setLanguages(languages);
        }

        Set<org.atlasapi.content.v2.model.udt.Certificate> certificates =
                internal.getCertificates();
        if (certificates != null) {
            content.setCertificates(certificates.stream()
                    .map(certificate::deserialize)
                    .collect(Collectors.toList()));
        }

        content.setYear(internal.getYear());

        content.setGenericDescription(internal.getGenericDescription());

        Set<Ref> eventRefs = internal.getEventRefs();
        if (eventRefs != null) {
            content.setEventRefs(eventRefs.stream()
                    .map(eventRef::deserialize)
                    .collect(Collectors.toList()));
        }
    }
}