package org.atlasapi.query.v4.event;

import java.io.IOException;

import org.atlasapi.entity.Person;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

public class PersonListWriter extends ContentGroupListWriter<Person> {

    @Override
    public String fieldName(Person entity) {
        return "participant";
    }

    @Override
    public void write(Person entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        super.write(entity, writer, ctxt);
        writer.writeField("given_name", entity.getGivenName());
        writer.writeField("family_name", entity.getFamilyName());
        writer.writeField("gender", entity.getGender());
        writer.writeField("birth_date", entity.getBirthDate().toString());
        writer.writeField("birth_place", entity.getBirthPlace());
        writer.writeField("quotes", entity.getQuotes());
    }

    @Override
    public String listName() {
        return "participants";
    }
}
