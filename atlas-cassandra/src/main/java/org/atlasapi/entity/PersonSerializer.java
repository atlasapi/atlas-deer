package org.atlasapi.entity;

import org.atlasapi.content.ContentGroupSerializer;
import org.atlasapi.serialization.protobuf.CommonProtos;

public class PersonSerializer implements Serializer<Person, CommonProtos.Person> {

    private final ContentGroupSerializer<Person> contentGroupSerializer = new ContentGroupSerializer<>();
    private final DateTimeSerializer dateTimeSerializer = new DateTimeSerializer();

    @Override
    public CommonProtos.Person serialize(Person person) {
        CommonProtos.Person.Builder builder = CommonProtos.Person.newBuilder();

        builder.setContentGroup(contentGroupSerializer.serialize(person));

        if (person.getGivenName() != null) {
            builder.setGivenName(person.getGivenName());
        }
        if (person.getFamilyName() != null) {
            builder.setFamilyName(person.getFamilyName());
        }
        if (person.getGender() != null) {
            builder.setGender(person.getGender());
        }
        if (person.getBirthDate() != null) {
            builder.setBirthDate(dateTimeSerializer.serialize(person.getBirthDate()));
        }
        if (person.getBirthPlace() != null) {
            builder.setBirthPlace(person.getBirthPlace());
        }
        if (person.getQuotes() != null) {
            builder.addAllQuote(person.getQuotes());
        }

        return builder.build();
    }

    @Override
    public Person deserialize(CommonProtos.Person msg) {
        Person person = new Person();

        contentGroupSerializer.deserialize(msg.getContentGroup(), person);

        if (msg.hasGivenName()) {
            person.setGivenName(msg.getGivenName());
        }
        if (msg.hasFamilyName()) {
            person.setFamilyName(msg.getFamilyName());
        }
        if (msg.hasGender()) {
            person.setGender(msg.getGender());
        }
        if (msg.hasBirthDate()) {
            person.setBirthDate(dateTimeSerializer.deserialize(msg.getBirthDate()));
        }
        if(msg.hasBirthPlace()) {
            person.setBirthPlace(msg.getBirthPlace());
        }
        person.setQuotes(msg.getQuoteList());

        return person;
    }
}
