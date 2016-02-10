package org.atlasapi.output.annotation;

import java.io.IOException;

import org.atlasapi.application.ApplicationSources;
import org.atlasapi.content.Content;
import org.atlasapi.media.product.Product;
import org.atlasapi.media.product.ProductResolver;
import org.atlasapi.output.EntityListWriter;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

public class ProductsAnnotation extends OutputAnnotation<Content> {

    private final ProductResolver productResolver;

    public ProductsAnnotation(ProductResolver productResolver) {
        super();
        this.productResolver = productResolver;
    }

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {
        writer.writeList(new EntityListWriter<Product>() {

            @Override
            public void write(Product entity, FieldWriter writer, OutputContext ctxt)
                    throws IOException {

            }

            @Override
            public String listName() {
                return "products";
            }

            @Override
            public String fieldName(Product entity) {
                return "product";
            }

        }, resolveProductsFor(entity, null), ctxt);
    }

    private Iterable<Product> resolveProductsFor(Content content,
            final ApplicationSources appSources) {
        return filter(productResolver.productsForContent(content.getCanonicalUri()), appSources);
    }

    private Iterable<Product> filter(Iterable<Product> productsForContent,
            final ApplicationSources appSources) {
        return Iterables.filter(productsForContent, new Predicate<Product>() {

            @Override
            public boolean apply(Product input) {
                return appSources.isReadEnabled(input.getPublisher());
            }
        });
    }

}
