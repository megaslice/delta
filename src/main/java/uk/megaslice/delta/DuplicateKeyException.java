package uk.megaslice.delta;

/**
 * {@link java.lang.RuntimeException} thrown when a dataset contains multiple items with the same natural key, or when
 * a delta contains an insert operation that would result in a duplicate key when applied.
 */
public final class DuplicateKeyException extends RuntimeException {

    DuplicateKeyException(String message) {
        super(message);
    }
}
