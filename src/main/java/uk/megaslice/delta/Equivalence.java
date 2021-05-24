package uk.megaslice.delta;

/**
 * A function to determine whether two dataset items are equivalent.
 *
 * @param <T>  the type of the dataset item
 */
@FunctionalInterface
public interface Equivalence<T> {

    /**
     * Returns a default item equivalence function using {@link Object#equals}
     *
     * @param <T>  the type of the dataset item
     * @return  a default item equivalence function
     */
    static <T> Equivalence<T> defaultEquivalence() {
        return Object::equals;
    }

    /**
     * Returns true if two given dataset items are equivalent, false otherwise.
     *
     * @param left   the first item
     * @param right  the second item
     * @return  true if {@code left} and {@code right} are equivalent, false otherwise
     */
    boolean isEquivalent(T left, T right);
}
