package uk.megaslice.delta;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import static java.util.Collections.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ApplyTests {

    @ParameterizedTest
    @MethodSource("uk.megaslice.delta.Generators#genItems")
    void emptyDelta(List<Item> items) {
        Collection<Item> result = Delta.<Item, String>empty().apply(items, Item.naturalKey);

        assertEquals(items.size(), result.size());
        assertEquals(new HashSet<>(items), new HashSet<>(result));
    }

    @ParameterizedTest
    @MethodSource("uk.megaslice.delta.Generators#genScenario")
    void roundTrip(Scenario scenario) {
        List<Item> before = scenario.before();
        List<Item> after = scenario.after();
        Delta<Item, String> delta = Delta.diff(before, after, Item.naturalKey);

        Collection<Item> result = delta.apply(before, Item.naturalKey);

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

        assertThrows(DuplicateKeyException.class, () -> delta.apply(beforeWithDuplicate, Item.naturalKey));
    }

    @ParameterizedTest
    @MethodSource("uk.megaslice.delta.Generators#genNonEmptyItems")
    void clashingInsert(List<Item> items) {
        Delta<Item, String> insertOnlyDelta = Delta.diff(emptySet(), singleton(items.get(0)), Item.naturalKey);

        assertThrows(DuplicateKeyException.class, () -> insertOnlyDelta.apply(items, Item.naturalKey));
    }
}
