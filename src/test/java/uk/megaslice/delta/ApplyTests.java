package uk.megaslice.delta;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.*;

import static java.util.Collections.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ApplyTests {

    private Collection<Item> apply(Delta<Item, String> delta, Collection<Item> items) {
        Collection<Item> result = delta.apply(items, Item.naturalKey);
        Map<String, Item> resultMap = delta.apply(Item.toMap(items));

        assertEquals(Item.toMap(result), resultMap);

        return result;
    }

    @Test
    void nullItems() {
        assertThrows(NullPointerException.class, () -> Delta.empty().apply(null));
        assertThrows(NullPointerException.class, () -> Delta.<Item, String>empty().apply(null, Item.naturalKey));
    }

    @Test
    void nullNaturalKey() {
        assertThrows(NullPointerException.class, () -> Delta.<Item, String>empty().apply(emptyList(), null));
    }

    @ParameterizedTest
    @MethodSource("uk.megaslice.delta.Generators#genItems")
    void nullInItems(List<Item> items) {
        items.add(null);
        shuffle(items, Generators.random);

        assertThrows(NullPointerException.class, () -> Delta.<Item, String>empty().apply(items, Item.naturalKey));
    }

    @ParameterizedTest
    @MethodSource("uk.megaslice.delta.Generators#genItems")
    void nullValueInItemsMap(List<Item> items) {
        Map<String, Item> itemsMap = Item.toMap(items);
        itemsMap.put("", null);

        assertThrows(NullPointerException.class, () -> Delta.<Item, String>empty().apply(itemsMap));
    }

    @ParameterizedTest
    @MethodSource("uk.megaslice.delta.Generators#genItems")
    void nullKeyInItemsMap(List<Item> items) {
        Map<String, Item> itemsMap = Item.toMap(items);
        itemsMap.put(null, Generators.genItem());

        assertThrows(NullPointerException.class, () -> Delta.<Item, String>empty().apply(itemsMap));
    }

    @ParameterizedTest
    @MethodSource("uk.megaslice.delta.Generators#genItems")
    void emptyDelta(List<Item> items) {
        Collection<Item> result = apply(Delta.empty(), items);

        assertEquals(items.size(), result.size());
        assertEquals(new HashSet<>(items), new HashSet<>(result));
    }

    @ParameterizedTest
    @MethodSource("uk.megaslice.delta.Generators#genScenario")
    void roundTrip(Scenario scenario) {
        List<Item> before = scenario.before();
        List<Item> after = scenario.after();
        Delta<Item, String> delta = Delta.diff(before, after, Item.naturalKey);

        Collection<Item> result = apply(delta, before);

        assertEquals(after.size(), result.size());
        assertEquals(new HashSet<>(after), new HashSet<>(result));
    }

    @ParameterizedTest
    @MethodSource("uk.megaslice.delta.Generators#genScenario")
    void duplicateKeysInItems(Scenario scenario) {
        Delta<Item, String> delta = Delta.diff(scenario.before(), scenario.after(), Item.naturalKey);
        List<Item> beforeWithDuplicate = new ArrayList<>(scenario.before());
        beforeWithDuplicate.add(beforeWithDuplicate.get(0));
        shuffle(beforeWithDuplicate, Generators.random);

        assertThrows(DuplicateKeyException.class, () -> apply(delta, beforeWithDuplicate));
    }

    @ParameterizedTest
    @MethodSource("uk.megaslice.delta.Generators#genNonEmptyItems")
    void clashingInsert(List<Item> items) {
        Delta<Item, String> insertOnlyDelta = Delta.diff(emptySet(), singleton(items.get(0)), Item.naturalKey);

        assertThrows(DuplicateKeyException.class, () -> insertOnlyDelta.apply(items, Item.naturalKey));
        assertThrows(DuplicateKeyException.class, () -> insertOnlyDelta.apply(Item.toMap(items)));
    }
}
