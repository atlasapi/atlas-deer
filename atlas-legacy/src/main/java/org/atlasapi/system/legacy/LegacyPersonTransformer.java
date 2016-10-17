package org.atlasapi.system.legacy;

import java.util.stream.Collectors;

import org.atlasapi.entity.Alias;
import org.atlasapi.media.entity.Person;

public class LegacyPersonTransformer extends DescribedLegacyResourceTransformer<
        Person, org.atlasapi.entity.Person> {

    @Override
    protected org.atlasapi.entity.Person createDescribed(Person input) {
        org.atlasapi.entity.Person person = new org.atlasapi.entity.Person();

        LegacyContentGroupTransformer.transformInto(person, input);

        person.setGivenName(input.getGivenName());
        person.setFamilyName(input.getFamilyName());
        person.setGender(input.getGender());
        person.setBirthDate(input.getBirthDate());
        person.setBirthPlace(input.getBirthPlace());
        person.setQuotes(input.getQuotes());

        person.setPseudoSurname(input.getPseudoSurname());
        person.setPseudoForename(input.getPseudoForename());
        person.setPersonSource(input.getSource());
        person.setSourceTitle(input.getSourceTitle());
        person.setAdditionalInfo(input.getAdditionalInfo());
        person.setBilling(input.getBilling());

        return person;
    }

    @Override
    protected Iterable<Alias> moreAliases(Person input) {
        return input.getAliases().stream()
                .map(alias -> new Alias(alias.getNamespace(), alias.getValue()))
                .collect(Collectors.toList());
    }
}
