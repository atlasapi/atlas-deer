package org.atlasapi.content;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.atlasapi.serialization.protobuf.ContentProtos;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.media.MimeType;


public class EncodingSerializerTest {

    private final EncodingSerializer serializer = new EncodingSerializer();
    
    @Test
    public void testDeSerializeEncoding() {
        Encoding encoding = new Encoding();
        encoding.setAvailableAt(ImmutableSet.of(new Location()));
        encoding.setAdvertisingDuration(1345);
        encoding.setAudioBitRate(4);
        encoding.setAudioChannels(5);
        encoding.setAudioCoding(MimeType.AUDIO_MP3);
        encoding.setBitRate(1);
        encoding.setContainsAdvertising(true);
        encoding.setDataContainerFormat(MimeType.VIDEO_MP4);
        encoding.setDataSize(1234L);
        encoding.setDistributor("distributor");
        encoding.setHasDOG(true);
        encoding.setSource("source");
        encoding.setVideoAspectRatio("16:9");
        encoding.setVideoBitRate(1);
        encoding.setVideoCoding(MimeType.VIDEO_H264);
        encoding.setVideoFrameRate(4.5f);
        encoding.setVideoHorizontalSize(6);
        encoding.setVideoProgressiveScan(true);
        encoding.setVideoVerticalSize(5);
        encoding.set3d(true);
        encoding.setVersionId("version");
        encoding.setQuality(Quality.SD);
        encoding.setQualityDetail("quality_detail");
        
        ContentProtos.Encoding serialized = serializer.serialize(encoding).build();
        Encoding deserialized = serializer.deserialize(serialized);
        
        assertThat(deserialized.getAvailableAt().size(), is(1));
        assertThat(deserialized.getAdvertisingDuration(), is(encoding.getAdvertisingDuration()));
        assertThat(deserialized.getAudioBitRate(), is(encoding.getAudioBitRate()));
        assertThat(deserialized.getAudioChannels(), is(encoding.getAudioChannels()));
        assertThat(deserialized.getAudioCoding(), is(encoding.getAudioCoding()));
        assertThat(deserialized.getBitRate(), is(encoding.getBitRate()));
        assertThat(deserialized.getContainsAdvertising(), is(encoding.getContainsAdvertising()));
        assertThat(deserialized.getDataContainerFormat(), is(encoding.getDataContainerFormat()));
        assertThat(deserialized.getDataSize(), is(encoding.getDataSize()));
        assertThat(deserialized.getDistributor(), is(encoding.getDistributor()));
        assertThat(deserialized.getHasDOG(), is(encoding.getHasDOG()));
        assertThat(deserialized.getSource(), is(encoding.getSource()));
        assertThat(deserialized.getVideoAspectRatio(), is(encoding.getVideoAspectRatio()));
        assertThat(deserialized.getVideoBitRate(), is(encoding.getVideoBitRate()));
        assertThat(deserialized.getVideoCoding(), is(encoding.getVideoCoding()));
        assertThat(deserialized.getVideoFrameRate(), is(encoding.getVideoFrameRate()));
        assertThat(deserialized.getVideoHorizontalSize(), is(encoding.getVideoHorizontalSize()));
        assertThat(deserialized.getVideoProgressiveScan(), is(encoding.getVideoProgressiveScan()));
        assertThat(deserialized.getVideoVerticalSize(), is(encoding.getVideoVerticalSize()));
        assertThat(deserialized.is3d(), is(encoding.is3d()));
        assertThat(deserialized.getVersionId(), is(encoding.getVersionId()));
        assertThat(deserialized.getQuality(), is(encoding.getQuality()));
        assertThat(deserialized.getQualityDetail(), is(encoding.getQualityDetail()));
        
    }

}
