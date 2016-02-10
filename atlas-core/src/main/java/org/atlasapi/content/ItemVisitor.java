package org.atlasapi.content;

public interface ItemVisitor<VALUE> {

    VALUE visit(Episode episode);

    VALUE visit(Film film);

    VALUE visit(Song song);

    VALUE visit(Item item);

    VALUE visit(Clip clip);

}
