package uk.megaslice.delta;

public interface Equivalence<T> {

    static <T> Equivalence<T> defaultEquivalence() {
        return Object::equals;
    }

    boolean isEquivalent(T left, T right);
}
