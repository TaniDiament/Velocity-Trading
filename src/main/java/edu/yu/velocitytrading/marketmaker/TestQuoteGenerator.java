package edu.yu.velocitytrading.marketmaker;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import edu.yu.velocitytrading.model.*;

@Component
@Profile("test-quote-generator")
public class TestQuoteGenerator implements QuoteGenerator {

    private final Map<String, AtomicInteger> generatedCountsBySymbol = new ConcurrentHashMap<>();
    private final Map<String, Quote> lastQuoteBySymbol = new ConcurrentHashMap<>();

    @Override
    public Quote generateQuote(Position position, Fill lastFill) {
        double referencePrice = lastFill != null ? lastFill.price() : 100.0;
        int quantity = Math.max(1, Math.abs(position.netQuantity()));
        Quote quote = new Quote(
                position.symbol(),
                Math.max(0.01, referencePrice - 0.05),
                quantity,
                referencePrice + 0.05,
                quantity,
                UUID.randomUUID(),
                System.currentTimeMillis() + 30_000
        );
        generatedCountsBySymbol.computeIfAbsent(position.symbol(), k -> new AtomicInteger()).incrementAndGet();
        lastQuoteBySymbol.put(position.symbol(), quote);
        return quote;
    }

    public int getGeneratedCount(String symbol) {
        AtomicInteger count = generatedCountsBySymbol.get(symbol);
        return count == null ? 0 : count.get();
    }

    public Quote getLastQuote(String symbol) {
        return lastQuoteBySymbol.get(symbol);
    }
}
