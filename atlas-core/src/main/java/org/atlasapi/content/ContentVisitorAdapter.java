package org.atlasapi.content;

/**
 * @param <VALUE> - the type of the result of visiting a {@link Content}
 */
public abstract class ContentVisitorAdapter<VALUE> implements ContentVisitor<VALUE> {

    @Override
    public VALUE visit(Brand brand) {
        return visitContainer(brand);
    }

    @Override
    public VALUE visit(Series series) {
        return visitContainer(series);
    }

    @Override
    public VALUE visit(Episode episode) {
        return visitItem(episode);
    }

    @Override
    public VALUE visit(Film film) {
        return visitItem(film);
    }

    @Override
    public VALUE visit(Song song) {
        return visitItem(song);
    }

    @Override
    public VALUE visit(Item item) {
        return visitItem(item);
    }

    @Override
    public VALUE visit(Clip clip) {
        return visitItem(clip);
    }

    /**
     * @param item
     */
    protected VALUE visitItem(Item item) {
        return null;
    }

    /**
     * @param container
     */
    protected VALUE visitContainer(Container container) {
        return null;
    }
}
