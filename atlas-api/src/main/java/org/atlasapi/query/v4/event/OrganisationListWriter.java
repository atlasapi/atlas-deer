package org.atlasapi.query.v4.event;


import org.atlasapi.entity.Person;
import org.atlasapi.event.Organisation;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import java.io.IOException;

public class OrganisationListWriter extends ContentGroupListWriter<Organisation> {
    private final EntityListWriter<Person> personListWriter;

    public OrganisationListWriter(EntityListWriter<Person> personListWriter) {
        this.personListWriter = personListWriter;
    }
    @Override
    public String fieldName(Organisation entity) {
        return "organisation";
    }

    @Override
    public void write(Organisation entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        super.write(entity, writer, ctxt);
        writer.writeList(personListWriter, entity.members(), ctxt);
    }

    @Override
    public String listName() {
        return "organisations";
    }
}
