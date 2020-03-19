package uk.megaslice.delta;

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static java.lang.Math.max;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

public class Generators {

    public static Stream<Scenario> genScenario() {
        return Stream.generate(() -> {
            Random random = new Random();
            Scenario scenario = new Scenario();

            scenario.added = genItems().findFirst().orElse(emptyList());
            scenario.unchanged = genItems().findFirst().orElse(emptyList());
            scenario.removed = genItems().findFirst().orElse(emptyList());

            scenario.beforeValueChanged = genItems().findFirst().orElse(emptyList());
            scenario.afterValueChanged = scenario.beforeValueChanged.stream()
                    .map(Generators::copyItemWithRandomValue)
                    .collect(toList());

            scenario.beforeKeyChanged = genItems().findFirst().orElse(emptyList());
            scenario.afterKeyChanged = scenario.beforeKeyChanged.stream()
                    .map(item -> item.withKey(String.valueOf(id.incrementAndGet())))
                    .collect(toList());

            return scenario;
        }).filter(scenario -> !scenario.before().isEmpty() && !scenario.after().isEmpty())
                .limit(STREAM_SIZE);
    }

    static final Random random = new Random();

    private static final AtomicLong id = new AtomicLong(0L);

    private static final int MAX_ITEM_COUNT = 20;

    private static final int STREAM_SIZE = 100;

    public static Stream<List<Item>> genNonEmptyItems() {
        return Stream.generate(() ->
                Stream.generate(Generators::genItem).limit(max(1, random.nextInt(MAX_ITEM_COUNT))).collect(toList()))
                .limit(STREAM_SIZE);
    }

    public static Stream<List<Item>> genItems() {
        return Stream.generate(() ->
                Stream.generate(Generators::genItem).limit(random.nextInt(MAX_ITEM_COUNT)).collect(toList()))
                .limit(STREAM_SIZE);
    }

    public static Item genItem() {
        long nextId = id.incrementAndGet();
        return new Item(nextId, String.valueOf(nextId), random.nextInt());
    }

    public static Item copyItemWithRandomValue(Item item) {
        int newValue;
        do {
            newValue = random.nextInt();
        } while (item.value == newValue);

        return item.withValue(newValue);
    }
}
