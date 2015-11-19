package org.atlasapi.system.legacy;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.atlasapi.content.ContentRef;
import org.atlasapi.content.ContentType;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.media.entity.EntityType;
import org.atlasapi.media.entity.Organisation;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class LegacyOrganisationTransformerTest {

    private LegacyOrganisationTransformer transformer;

    @Before
    public void setUp() throws Exception {
        transformer = new LegacyOrganisationTransformer();
    }

    @Test
    public void testTransformation() throws Exception {
        Organisation input = getOrganisation();

        org.atlasapi.organisation.Organisation organisation = transformer.apply(input);

        checkOrganisation(organisation, input);
    }

    private Organisation getOrganisation() {
        Organisation organisation = new Organisation();

        organisation.setTitle("title");
        organisation.setPublisher(Publisher.BBC);

        organisation.setType(ContentGroup.Type.ORGANISATION);
        organisation.setContents(ImmutableList.of(
                new ChildRef(1111L, "uri", "sort", DateTime.now(), EntityType.ITEM)
        ));

        Person person = new Person();
        person.setId(2222L);
        organisation.setMembers(ImmutableList.of(person));

        return organisation;
    }

    private void checkOrganisation(org.atlasapi.organisation.Organisation organisation, Organisation input) {
        assertThat(organisation.getTitle(), is(input.getTitle()));
        assertThat(organisation.getType(), is(org.atlasapi.content.ContentGroup.Type.ORGANISATION));
        checkContent(organisation, input);
        assertThat(organisation.members().size(), is(input.members().size()));
        assertThat(organisation.members().get(0).getId().longValue(), is(input.members().get(0).getId()));
    }

    private void checkContent(org.atlasapi.organisation.Organisation organisation, Organisation input) {
        ContentRef content = organisation.getContents().get(0);
        ChildRef inputContent = input.getContents().get(0);

        assertThat(content.getId().longValue(), is(inputContent.getId()));
        assertThat(content.getSource(), is(organisation.getSource()));
        assertThat(content.getContentType(), is(ContentType.ITEM));
    }
}