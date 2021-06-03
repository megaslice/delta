package uk.megaslice.delta;

import java.util.Objects;

/**
 * A function to distill an item down to its essential features for equivalence comparisons.
 *
 * Implementations will typically return a copy of the item with fields defaulted where they are inessential
 * for comparisons.
 *
 * @param <T>  the type of dataset item
 * @param <U>  the distilled type for equivalence comparisons
 */
@FunctionalInterface
public interface Essence<T, U> {

    /**
     * Return a distilled item for equivalence comparisons.
     *
     * @param value the item to distill
     * @return  a distilled item for equivalence comparisons
     */
    U distill(T value);

    /**
     * Returns an {@link Equivalence} function derived from this instance, using default equivalence for
     * the distilled items.
     *
     * @return  an {@link Equivalence} function derived from this instance
     */
    default Equivalence<T> asEquivalence() {
        return asEquivalence(Equivalence.defaultEquivalence());
    }

    /**
     * Returns an {@link Equivalence} function derived from this instance.
     *
     * @param essenceEquivalence  an {@link Equivalence} function used to compare the distilled items
     * @return  an {@link Equivalence} function derived from this instance
     */
    default Equivalence<T> asEquivalence(Equivalence<U> essenceEquivalence) {
        Objects.requireNonNull(essenceEquivalence, "essenceEquivalence must not be null");
        return (left, right) -> essenceEquivalence.isEquivalent(distill(left), distill(right));
    }
}
