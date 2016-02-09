package org.atlasapi.content;

import org.atlasapi.content.Image.AspectRatio;
import org.atlasapi.content.Image.Builder;
import org.atlasapi.content.Image.Color;
import org.atlasapi.content.Image.Theme;
import org.atlasapi.content.Image.Type;

import com.metabroadcast.common.media.MimeType;

import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ImageSerializerTest {

    private final ImageSerializer serializer = new ImageSerializer();

    @Test
    public void testImageSerializationAndDeserialization() {
        Image image = getImage();

        Image deserialized = serializer.deserialize(serializer.serialize(image));

        checkImage(deserialized, image);
    }

    public Image getImage() {
        Builder builder = Image.builder("http://example.org/");
        builder.withAspectRatio(AspectRatio.FOUR_BY_THREE);
        builder.withAvailabilityEnd(new DateTime(
                2014,
                DateTimeConstants.JANUARY,
                1,
                0,
                0,
                0,
                0
        ).withZone(
                DateTimeZone.UTC));
        builder.withAvailabilityStart(new DateTime(
                2013,
                DateTimeConstants.JANUARY,
                1,
                0,
                0,
                0,
                0
        ).withZone(DateTimeZone.UTC));
        builder.withColor(Color.BLACK_AND_WHITE);
        builder.withHasTitleArt(false);
        builder.withHeight(123);
        builder.withMimeType(MimeType.APPLICATION_ATOM_XML);
        builder.withTheme(Theme.DARK_OPAQUE);
        builder.withType(Type.ADDITIONAL);
        builder.withWidth(6);

        return builder.build();
    }

    public void checkImage(Image actual, Image expected) {
        assertThat(actual.getAspectRatio(), is(equalTo(expected.getAspectRatio())));
        assertThat(actual.getAvailabilityStart(), is(equalTo(expected.getAvailabilityStart())));
        assertThat(actual.getAvailabilityEnd(), is(equalTo(expected.getAvailabilityEnd())));
        assertThat(actual.getColor(), is(equalTo(expected.getColor())));
        assertThat(actual.hasTitleArt(), is(equalTo(expected.hasTitleArt())));
        assertThat(actual.getHeight(), is(equalTo(expected.getHeight())));
        assertThat(actual.getMimeType(), is(equalTo(expected.getMimeType())));
    }
}
