package org.atlasapi.content;

import org.atlasapi.entity.DateTimeSerializer;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.serialization.protobuf.CommonProtos;
import org.atlasapi.serialization.protobuf.CommonProtos.Reference;
import org.atlasapi.serialization.protobuf.CommonProtos.Reference.Builder;
import org.atlasapi.source.Sources;
import org.atlasapi.util.ImmutableCollectors;
import org.joda.time.DateTime;

import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;


public class ContentRefSerializer {

    private final Publisher deflt;
    private final CertificateSerializer certificateSerializer = new CertificateSerializer();

    public ContentRefSerializer(Publisher deflt) {
        this.deflt = deflt;
    }
    
    public CommonProtos.Reference.Builder serialize(final ContentRef contentRef) {
        return contentRef.getContentType().accept(new ContentType.Visitor<CommonProtos.Reference.Builder>() {

            private Builder serializeRef(ContentRef contentRef) {
                CommonProtos.Reference.Builder ref = CommonProtos.Reference.newBuilder();
                ref.setId(contentRef.getId().longValue());
                ref.setSource(contentRef.getSource().key());
                ref.setType(contentRef.getContentType().toString());
                return ref;
            }
            
            @Override
            public Builder visitBrand(ContentType contentType) {
                return serializeRef(contentRef);
            }

            @Override
            public Builder visitClip(ContentType contentType) {
                return visitItem(contentType);
            }

            @Override
            public Builder visitSong(ContentType contentType) {
                return visitItem(contentType);
            }

            @Override
            public Builder visitFilm(ContentType contentType) {
                return visitItem(contentType);
            }

            @Override
            public Builder visitEpisode(ContentType contentType) {
                return visitItem(contentType);
            }

            @Override
            public Builder visitItem(ContentType contentType) {
                Builder builder = serializeRef(contentRef);
                ItemRef itemRef = (ItemRef) contentRef;
                builder.setSort(itemRef.getSortKey());
                builder.setUpdated(new DateTimeSerializer().serialize(itemRef.getUpdated()));
                return builder;
            }

            @Override
            public Builder visitSeries(ContentType contentType) {
                Builder builder = serializeRef(contentRef);
                SeriesRef seriesRef = (SeriesRef) contentRef;
                if (seriesRef.getTitle() != null) {
                    builder.setSort(seriesRef.getTitle());
                }
                if (seriesRef.getSeriesNumber() != null) {
                    builder.setPosition(seriesRef.getSeriesNumber());
                }
                if (seriesRef.getUpdated() != null) {
                    builder.setUpdated(new DateTimeSerializer().serialize(seriesRef.getUpdated()));
                }
                if (seriesRef.getCertificates() != null) {
                    ImmutableSet<CommonProtos.Certificate> certs = seriesRef.getCertificates().stream()
                            .map(certificateSerializer::serialize)
                            .collect(ImmutableCollectors.toSet());
                    builder.addAllCertificates(certs);
                }
                if (seriesRef.getReleaseYear() != null) {
                    builder.setReleaseYear(seriesRef.getReleaseYear());
                }
                return builder;
            }
        });
    }
    
    
    public ContentRef deserialize(final Reference ref) {
        final Id id = Id.valueOf(ref.getId());
        final Publisher possibleSrc = Sources.fromPossibleKey(ref.getSource()).orNull();
        final Publisher src = possibleSrc != null ? possibleSrc : deflt; 
        final String sortKey = ref.getSort();
        final DateTime updated = new DateTimeSerializer().deserialize(ref.getUpdated());
        final int position = Ints.saturatedCast(ref.getPosition());
        ContentType type = ContentType.fromKey(ref.getType()).get();
        
        return type.accept(new ContentType.Visitor<ContentRef>() {
            
            @Override
            public ContentRef visitItem(ContentType contentType) {
                return new ItemRef(id, src, sortKey, updated);
            }
            
            @Override
            public ContentRef visitClip(ContentType contentType) {
                return new ClipRef(id, src, sortKey, updated);
            }
            
            @Override
            public ContentRef visitEpisode(ContentType contentType) {
                return new EpisodeRef(id, src, sortKey, updated);
            }
            
            @Override
            public ContentRef visitFilm(ContentType contentType) {
                return new FilmRef(id, src, sortKey, updated);
            }
            
            @Override
            public ContentRef visitSong(ContentType contentType) {
                return new SongRef(id, src, sortKey, updated);
            }
            
            @Override
            public ContentRef visitBrand(ContentType contentType) {
                return new BrandRef(id, src);
            }

            @Override
            public ContentRef visitSeries(ContentType contentType) {
                ImmutableSet<Certificate> certs = ref.getCertificatesList().stream()
                                .map(certificateSerializer::deserialize)
                                .collect(ImmutableCollectors.toSet());
                return new SeriesRef(id, src, sortKey, position, updated, ref.getReleaseYear(), certs);
            }

        });
    }
    
}
