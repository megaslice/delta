package uk.megaslice.delta;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Objects;
import java.util.Optional;

public abstract class Operation<T> {

    private Operation() {}

    enum Type {
        INSERT, UPDATE, DELETE
    }

    public abstract Type type();

    public Optional<T> oldItem() {
        return Optional.empty();
    }

    public Optional<T> newItem() {
        return Optional.empty();
    }

    public Optional<Operation<T>> combine(Operation<T> other) {
        return combine(other, Equivalence.defaultEquivalence());
    }

    public abstract Optional<Operation<T>> combine(Operation<T> other, Equivalence<T> equivalence);

    public static <T> Operation<T> insert(T item) {
        Objects.requireNonNull(item, "item must not be null");
        return new Insert<>(item);
    }

    public static <T> Operation<T> update(T before, T after) {
        Objects.requireNonNull(before, "before must not be null");
        Objects.requireNonNull(after, "after must not be null");
        return new Update<>(before, after);
    }

    public static <T> Operation<T> delete(T item) {
        Objects.requireNonNull(item, "item must not be null");
        return new Delete<>(item);
    }


    @ToString
    @EqualsAndHashCode(callSuper=false)
    private static class Insert<T> extends Operation<T> {
        private final T item;

        private Insert(T item) {
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

        @Override
        public Optional<Operation<T>> combine(Operation<T> other, Equivalence<T> equivalence) {
            if (other instanceof Insert) {
                throw new InvalidCombination(Type.INSERT, Type.INSERT);
            } else if (other instanceof Update) {
                return Optional.of(new Insert<>(((Update<T>) other).after));
            } else if (other instanceof Delete) {
                return Optional.empty();
            } else {
                throw new IllegalStateException("Unsupported operation type: " + other.getClass());
            }
        }
    }

    @ToString
    @EqualsAndHashCode(callSuper=false)
    private static class Update<T> extends Operation<T> {
        private final T before;
        private final T after;

        private Update(T before, T after) {
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

        @Override
        public Optional<Operation<T>> combine(Operation<T> other, Equivalence<T> equivalence) {
            if (other instanceof Insert) {
                throw new InvalidCombination(Type.UPDATE, Type.INSERT);
            } else if (other instanceof Update) {
                T otherAfter = ((Update<T>) other).after;
                return equivalence.isEquivalent(before, otherAfter) ?
                        Optional.empty() :
                        Optional.of(new Update<>(before, otherAfter));
            } else if (other instanceof Delete) {
                return Optional.of(other);
            } else {
                throw new IllegalStateException("Unsupported operation type: " + other.getClass());
            }
        }
    }

    @ToString
    @EqualsAndHashCode(callSuper=false)
    static class Delete<T> extends Operation<T> {
        private final T item;

        private Delete(T item) {
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

        @Override
        public Optional<Operation<T>> combine(Operation<T> other, Equivalence<T> equivalence) {
            if (other instanceof Insert) {
                T otherItem = ((Insert<T>) other).item;
                return equivalence.isEquivalent(item, otherItem) ?
                        Optional.empty() :
                        Optional.of(new Update<>(item, otherItem));
            } else if (other instanceof Update) {
                throw new InvalidCombination(Type.DELETE, Type.UPDATE);
            } else if (other instanceof Delete) {
                throw new InvalidCombination(Type.DELETE, Type.DELETE);
            } else {
                throw new IllegalStateException("Unsupported operation type: " + other.getClass());
            }
        }
    }
}