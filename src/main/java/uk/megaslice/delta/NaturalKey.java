package uk.megaslice.delta;

/**
 * A function to derive a natural key for a given dataset item.
 *
 * @param <T>  the type of the dataset item
 * @param <K>  the type of the dataset item's natural key
 */
@FunctionalInterface
public interface NaturalKey<T, K> {

    /**
     * Returns a natural key for the given dataset item.
     *
     * @param item  the dataset item for which a natural key is to be derived
     * @return  the natural key of the given item
     */
    K getNaturalKey(T item);
}
