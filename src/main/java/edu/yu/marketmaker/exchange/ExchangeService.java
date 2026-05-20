package edu.yu.marketmaker.exchange;

import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.*;

import edu.yu.marketmaker.memory.Repository;
import edu.yu.marketmaker.model.ExternalOrder;
import edu.yu.marketmaker.model.Quote;
import edu.yu.marketmaker.service.ServiceHealth;

/**
 * Exchange service manages quotes and external orders
 */
@RestController
@Profile("exchange")
public class ExchangeService {

    private Repository<String, Quote> quoteRepository;
    private OrderDispatcher orderDispatcher;
    private ReservationRequester reservationRequester;
    private final Logger logger;

    public ExchangeService(ReservationRequester reservationRequester, Repository<String, Quote> quoteRepository, OrderDispatcher orderDispatcher) {
        this.quoteRepository = quoteRepository;
        this.orderDispatcher = orderDispatcher;
        this.reservationRequester = reservationRequester;
        this.logger = LoggerFactory.getLogger(ExchangeService.class);
    }
    
    /**
     * HTTP: get a quote from symbol
     * @param symbol the symbol to get a quote for
     * @return the corresponding quote
     */
    @GetMapping("/quotes/{symbol}")
    Quote getQuote(@PathVariable String symbol) {
        Optional<Quote> quote = quoteRepository.get(symbol);
        if (quote.isPresent()) {
            return quote.get();
        } else {
            throw new QuoteNotFoundException(symbol);
        }
        
    }

    /**
     * HTTP: update the value of a quote
     * @param symbol the symbol to uodate a quote for
     * @param quote the new value of the quote
     */
    @PutMapping("/quotes/{symbol}")
    void putQuote(@PathVariable String symbol, @RequestBody Quote quote) {
        logger.info("Putting new version of quote for: {}, {}", symbol, quoteRepository.get(symbol).isPresent());
        if (quoteRepository.get(symbol).isEmpty()) { // this is used for bootstrapping the quotes
            logger.info("Bootstrapping quote for: {}", quote.symbol());
            reservationRequester.sendReservation(quote);
            quoteRepository.put(quote);
        }
    }

    /**
     * HTTP: submit an external order
     * @param order the external order to submit
     */
    @PostMapping("/orders")
    void submitOrder(@RequestBody ExternalOrder order) {
        orderDispatcher.dispatchOrder(order);
    }

    /**
     * HTTP: retrieve the health and info of the service
     * @return the current health info
     */
    @GetMapping("/health")
    ServiceHealth getHealth() {
        return new ServiceHealth(true, 0, "Exchange Service");
    }
}
