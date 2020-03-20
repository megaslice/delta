package uk.megaslice.delta;

public final class DuplicateKeyException extends RuntimeException {

    DuplicateKeyException(String message) {
        super(message);
    }
}
