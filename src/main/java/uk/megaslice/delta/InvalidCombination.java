package uk.megaslice.delta;

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
