package edu.yu.velocitytrading.marketmaker;

import edu.yu.velocitytrading.model.Quote;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@Profile("test-quote-generator")
@RequestMapping("/test/quote-generator")
public class TestQuoteGeneratorController {

    private final TestQuoteGenerator testQuoteGenerator;

    public TestQuoteGeneratorController(TestQuoteGenerator testQuoteGenerator) {
        this.testQuoteGenerator = testQuoteGenerator;
    }

    @GetMapping("/count/{symbol}")
    public ResponseEntity<Integer> getGeneratedCount(@PathVariable String symbol) {
        if (symbol == null || symbol.isBlank()) {
            return ResponseEntity.badRequest().build();
        }
        return ResponseEntity.ok(testQuoteGenerator.getGeneratedCount(symbol));
    }

    @GetMapping("/last/{symbol}")
    public ResponseEntity<Quote> getLastQuote(@PathVariable String symbol) {
        if (symbol == null || symbol.isBlank()) {
            return ResponseEntity.badRequest().build();
        }
        Quote quote = testQuoteGenerator.getLastQuote(symbol);
        if (quote == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(quote);
    }
}

