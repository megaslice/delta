package uk.megaslice.delta;

/**
 * {@link java.lang.RuntimeException} thrown when there is an attempt to combine operations that cannot be combined.
 *
 * @see Operation#combine(Operation)
 * @see Operation#combine(Operation, Equivalence)
 */
public final class InvalidCombination extends RuntimeException {

    private final Operation.Type left;
    private final Operation.Type right;

    InvalidCombination(Operation.Type left, Operation.Type right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public String getMessage() {
        return String.format("Can't combine %s with %s", left.name().toLowerCase(), right.name().toLowerCase());
    }
}
