package uk.megaslice.delta;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Objects;
import java.util.Optional;

public interface Operation<T> {
    enum Type {
        INSERT, UPDATE, DELETE
    }

    Type type();

    default Optional<T> oldItem() {
        return Optional.empty();
    }

    default Optional<T> newItem() {
        return Optional.empty();
    }

    static <T> Operation<T> insert(T item) {
        Objects.requireNonNull(item, "item must not be null");
        return new Insert<>(item);
    }

    static <T> Operation<T> update(T before, T after) {
        Objects.requireNonNull(before, "before must not be null");
        Objects.requireNonNull(after, "after must not be null");
        return new Update<>(before, after);
    }

    static <T> Operation<T> delete(T item) {
        Objects.requireNonNull(item, "item must not be null");
        return new Delete<>(item);
    }


    @ToString
    @EqualsAndHashCode
    class Insert<T> implements Operation<T> {
        private final T item;

        Insert(T item) {
            this.item = item;
        }

        @Override
        public Operation.Type type() {
            return Type.INSERT;
        }

        @Override
        public Optional<T> newItem() {
            return Optional.of(item);
        }
    }

    @ToString
    @EqualsAndHashCode
    class Update<T> implements Operation<T> {
        private final T before;
        private final T after;

        Update(T before, T after) {
            this.before = before;
            this.after = after;
        }

        @Override
        public Operation.Type type() {
            return Type.UPDATE;
        }

        @Override
        public Optional<T> oldItem() {
            return Optional.of(before);
        }

        @Override
        public Optional<T> newItem() {
            return Optional.of(after);
        }
    }

    @ToString
    @EqualsAndHashCode
    class Delete<T> implements Operation<T> {
        private final T item;

        Delete(T item) {
            this.item = item;
        }

        @Override
        public Operation.Type type() {
            return Type.DELETE;
        }

        @Override
        public Optional<T> oldItem() {
            return Optional.of(item);
        }
    }
}