package org.atlasapi.content;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.atlasapi.content.Image.AspectRatio;
import org.atlasapi.content.Image.Builder;
import org.atlasapi.content.Image.Color;
import org.atlasapi.content.Image.Theme;
import org.atlasapi.content.Image.Type;
import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import com.metabroadcast.common.media.MimeType;


public class ImageSerializerTest {

    private final ImageSerializer serializer = new ImageSerializer();
    
    @Test
    public void testImageSerializationAndDeserialization() {
        Builder builder = Image.builder("http://example.org/");
        builder.withAspectRatio(AspectRatio.FOUR_BY_THREE);
        builder.withAvailabilityEnd(new DateTime(2014, DateTimeConstants.JANUARY, 1, 0, 0, 0, 0).withZone(DateTimeZone.UTC));
        builder.withAvailabilityStart(new DateTime(2013, DateTimeConstants.JANUARY, 1, 0, 0, 0, 0).withZone(DateTimeZone.UTC));
        builder.withColor(Color.BLACK_AND_WHITE);
        builder.withHasTitleArt(true);
        builder.withHeight(123);
        builder.withMimeType(MimeType.APPLICATION_ATOM_XML);
        builder.withTheme(Theme.DARK_OPAQUE);
        builder.withType(Type.ADDITIONAL);
        builder.withWidth(6);
        
        Image image = builder.build();
        Image deserialized = serializer.deserialize(serializer.serialize(image).build());
        
        assertThat(deserialized.getAspectRatio(), is(equalTo(image.getAspectRatio())));
        assertThat(deserialized.getAvailabilityStart(), is(equalTo(image.getAvailabilityStart())));
        assertThat(deserialized.getAvailabilityEnd(), is(equalTo(image.getAvailabilityEnd())));
        assertThat(deserialized.getColor(), is(equalTo(image.getColor())));
        assertThat(deserialized.hasTitleArt(), is(equalTo(image.hasTitleArt())));
        assertThat(deserialized.getHeight(), is(equalTo(image.getHeight())));
        assertThat(deserialized.getMimeType(), is(equalTo(image.getMimeType())));
    }
}
