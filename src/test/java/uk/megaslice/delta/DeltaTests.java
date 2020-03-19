package uk.megaslice.delta;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static java.lang.Math.max;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;
import static org.junit.jupiter.api.Assertions.*;

class DeltaTests {

    @ToString
    @EqualsAndHashCode
    static class Item {
        final long id;
        final String key;
        final int value;

        Item(long id, String key, int value) {
            this.id = id;
            this.key = key;
            this.value = value;
        }

        Item withKey(String newKey) {
            return new Item(this.id, newKey, this.value);
        }

        Item withValue(int newValue) {
            return new Item(this.id, this.key, newValue);
        }
    }

    private static final NaturalKey<Item, String> itemNaturalKey = item -> item.key;

    @ToString
    static class Scenario {
        List<Item> added = emptyList();
        List<Item> unchanged = emptyList();
        List<Item> removed = emptyList();
        List<Item> beforeValueChanged = emptyList();
        List<Item> afterValueChanged = emptyList();
        List<Item> beforeKeyChanged = emptyList();
        List<Item> afterKeyChanged = emptyList();

        List<Item> before() {
            List<Item> items = new ArrayList<>();
            items.addAll(unchanged);
            items.addAll(beforeValueChanged);
            items.addAll(beforeKeyChanged);
            items.addAll(removed);
            shuffle(items, random);
            return unmodifiableList(items);
        }

        List<Item> after() {
            List<Item> items = new ArrayList<>();
            items.addAll(unchanged);
            items.addAll(afterValueChanged);
            items.addAll(afterKeyChanged);
            items.addAll(added);
            shuffle(items, random);
            return unmodifiableList(items);
        }
    }

    @Test
    void emptyInput() {
        Delta<Item, String> delta = Delta.diff(emptyList(), emptyList(), itemNaturalKey);

        assertEquals(Delta.<Item, String>empty(), delta);
        assertTrue(delta.isEmpty());
    }

    @ParameterizedTest
    @MethodSource("genItems")
    void equivalentInputs(List<Item> items) {
        List<Item> before = new ArrayList<>(items);
        List<Item> after = new ArrayList<>(items);
        shuffle(before, random);
        shuffle(after, random);

        Delta<Item, String> delta = Delta.diff(before, after, itemNaturalKey);

        assertEquals(Delta.<Item, String>empty(), delta);
        assertTrue(delta.isEmpty());
    }

    @ParameterizedTest
    @MethodSource("genItems")
    void sameInputReference(List<Item> items) {
        Delta<Item, String> delta = Delta.diff(items, items, itemNaturalKey);

        assertEquals(Delta.<Item, String>empty(), delta);
        assertTrue(delta.isEmpty());
    }

    @ParameterizedTest
    @MethodSource("genNonEmptyItems")
    void emptyBeforeNonEmptyAfter(List<Item> items) {
        Delta<Item, String> delta = Delta.diff(emptyList(), items, itemNaturalKey);

        assertTrue(delta.operations().values().stream().allMatch(op -> op.type() == Operation.Type.INSERT));
    }

    @ParameterizedTest
    @MethodSource("genNonEmptyItems")
    void nonEmptyBeforeEmptyAfter(List<Item> items) {
        Delta<Item, String> delta = Delta.diff(items, emptyList(), itemNaturalKey);

        assertTrue(delta.operations().values().stream().allMatch(op -> op.type() == Operation.Type.DELETE));
    }

    @ParameterizedTest
    @MethodSource("genScenario")
    void addedItemsAndChangedKeys(Scenario scenario) {
        Delta<Item, String> delta = Delta.diff(scenario.before(), scenario.after(), itemNaturalKey);

        Set<Operation<Item>> expectedInserts = concat(scenario.added.stream(), scenario.afterKeyChanged.stream())
                .map(Operation::insert)
                .collect(toSet());

        Set<Operation<Item>> actualInserts = delta.operations().values().stream()
                .filter(op -> op.type() == Operation.Type.INSERT)
                .collect(toSet());

        assertEquals(expectedInserts, actualInserts);
    }

    @ParameterizedTest
    @MethodSource("genScenario")
    void changedValues(Scenario scenario) {
        Delta<Item, String> delta = Delta.diff(scenario.before(), scenario.after(), itemNaturalKey);

        Set<Operation<Item>> expectedUpdates = new HashSet<>();
        for (int i = 0; i < scenario.beforeValueChanged.size(); i++) {
            Item before = scenario.beforeValueChanged.get(i);
            Item after = scenario.afterValueChanged.get(i);
            expectedUpdates.add(Operation.update(before, after));
        }

        Set<Operation<Item>> actualUpdates = delta.operations().values().stream()
                .filter(op -> op.type() == Operation.Type.UPDATE)
                .collect(toSet());

        assertEquals(expectedUpdates, actualUpdates);
    }

    @ParameterizedTest
    @MethodSource("genScenario")
    void unchangedItems(Scenario scenario) {
        Delta<Item, String> delta = Delta.diff(scenario.before(), scenario.after(), itemNaturalKey);

        Set<Item> unchanged = new HashSet<>(scenario.unchanged);

        assertTrue(delta.operations().values().stream().noneMatch(op ->
                op.oldItem().map(unchanged::contains).orElse(false) ||
                        op.newItem().map(unchanged::contains).orElse(false)));
    }

    @ParameterizedTest
    @MethodSource("genScenario")
    void removedItemsAndChangedKeys(Scenario scenario) {
        Delta<Item, String> delta = Delta.diff(scenario.before(), scenario.after(), itemNaturalKey);

        Set<Operation<Item>> expectedDeletes = concat(scenario.removed.stream(), scenario.beforeKeyChanged.stream())
                .map(Operation::delete)
                .collect(toSet());

        Set<Operation<Item>> actualDeletes = delta.operations().values().stream()
                .filter(op -> op.type() == Operation.Type.DELETE)
                .collect(toSet());

        assertEquals(expectedDeletes, actualDeletes);
    }

    @ParameterizedTest
    @MethodSource("genScenario")
    void duplicateKeysInBeforeItems(Scenario scenario) {
        List<Item> beforeWithDuplicate = new ArrayList<>(scenario.before());
        beforeWithDuplicate.add(beforeWithDuplicate.get(0));
        shuffle(beforeWithDuplicate, random);

        assertThrows(DuplicateKeyException.class,
                () -> Delta.diff(beforeWithDuplicate, scenario.after(), itemNaturalKey));
    }

    @ParameterizedTest
    @MethodSource("genScenario")
    void duplicateKeysInAfterItems(Scenario scenario) {
        List<Item> afterWithDuplicate = new ArrayList<>(scenario.after());
        afterWithDuplicate.add(afterWithDuplicate.get(0));
        shuffle(afterWithDuplicate, random);

        assertThrows(DuplicateKeyException.class,
                () -> Delta.diff(scenario.before(), afterWithDuplicate, itemNaturalKey));
    }

    static Stream<Scenario> genScenario() {
        return Stream.generate(() -> {
            Random random = new Random();
            Scenario scenario = new Scenario();

            scenario.added = genItems().findFirst().orElse(emptyList());
            scenario.unchanged = genItems().findFirst().orElse(emptyList());
            scenario.removed = genItems().findFirst().orElse(emptyList());

            scenario.beforeValueChanged = genItems().findFirst().orElse(emptyList());
            scenario.afterValueChanged = scenario.beforeValueChanged.stream().map(item -> {
                int newValue;
                do {
                    newValue = random.nextInt();
                } while (item.value == newValue);

                return item.withValue(newValue);
            }).collect(toList());

            scenario.beforeKeyChanged = genItems().findFirst().orElse(emptyList());
            scenario.afterKeyChanged = scenario.beforeKeyChanged.stream()
                    .map(item -> item.withKey(String.valueOf(id.incrementAndGet())))
                    .collect(toList());

            return scenario;
        }).filter(scenario -> !scenario.before().isEmpty() && !scenario.after().isEmpty())
                .limit(STREAM_SIZE);
    }

    private static final Random random = new Random();

    private static final AtomicLong id = new AtomicLong(0L);

    private static final int MAX_ITEM_COUNT = 20;

    private static final int STREAM_SIZE = 100;

    static Stream<List<Item>> genNonEmptyItems() {
        return Stream.generate(() ->
                genItem().limit(max(1, random.nextInt(MAX_ITEM_COUNT))).collect(toList()))
                .limit(STREAM_SIZE);
    }

    static Stream<List<Item>> genItems() {
        return Stream.generate(() ->
                genItem().limit(random.nextInt(MAX_ITEM_COUNT)).collect(toList()))
                .limit(STREAM_SIZE);
    }

    static Stream<Item> genItem() {
        return Stream.generate(() -> {
            long nextId = id.incrementAndGet();
            return new Item(nextId, String.valueOf(nextId), random.nextInt());
        });
    }
}
