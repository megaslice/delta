package uk.megaslice.delta;

public interface NaturalKey<T, K> {

    K getNaturalKey(T item);
}
