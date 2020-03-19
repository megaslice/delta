package uk.megaslice.delta;

import java.util.Objects;

public interface Equivalence<T> {

    static <T> Equivalence<T> defaultEquivalence() {
        return (left, right) -> {
            Objects.requireNonNull(left, "left must not be null");
            Objects.requireNonNull(right, "right must not be null");
            return left.equals(right);
        };
    }

    boolean isEquivalent(T left, T right);
}
