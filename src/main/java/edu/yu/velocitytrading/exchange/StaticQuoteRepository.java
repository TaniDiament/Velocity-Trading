package edu.yu.velocitytrading.exchange;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import edu.yu.velocitytrading.memory.Repository;
import edu.yu.velocitytrading.model.Quote;

@Component
@Profile("testing")
public class StaticQuoteRepository implements Repository<String, Quote> {

    private final Map<String, Quote> map = new ConcurrentHashMap<>();

    public StaticQuoteRepository() {
        this.map.putAll(generateQuotes(new Random(1234)));
    }

    private static Map<String, Quote> generateQuotes(Random random) {
        String[] symbols = new String[]{"AAA", "BBB", "CCC", "DDD", "EEE", "FFF", "ABC", "DEF", "XYZ"};
        Map<String, Quote> quotes = new ConcurrentHashMap<>();
        for (int i = 0; i < symbols.length; i++) {
            quotes.put(symbols[i], new Quote(symbols[i],
                random.nextInt(100), random.nextInt(100), random.nextInt(100), random.nextInt(100),
                UUID.randomUUID(), random.nextInt(1000)
            ));
        }
        return quotes;
    }

    @Override
    public Optional<Quote> get(String id) {
        return Optional.ofNullable(map.get(id));
    }

    @Override
    public void put(Quote entity) {
        map.put(entity.getId(), entity);
    }

    @Override
    public Collection<Quote> getAll() {
        return map.values();
    }

    @Override
    public void delete(String id) {
        map.remove(id);
    }
    
}
