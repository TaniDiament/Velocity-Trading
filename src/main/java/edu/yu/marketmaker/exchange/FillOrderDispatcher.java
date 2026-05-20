package edu.yu.marketmaker.exchange;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.*;
import org.springframework.context.annotation.Profile;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.stereotype.Component;
import edu.yu.marketmaker.memory.Repository;
import edu.yu.marketmaker.model.*;

import java.util.*;
import java.util.concurrent.*;

@Component
@Profile("exchange")
public class FillOrderDispatcher implements OrderDispatcher {

    private static final Logger logger = LoggerFactory.getLogger(FillOrderDispatcher.class);

    private final Repository<String, Quote> quoteRepository;
    private final FillSender fillSender;
    private final RSocketRequester reservationRequester;
    private final Map<String, Object> symbolLocks = new ConcurrentHashMap<>();

    // Backward-compatible constructor used by isolated unit tests.
    public FillOrderDispatcher(Repository<String, Quote> repository, FillSender fillSender) {
        this.quoteRepository = repository;
        this.fillSender = fillSender;
        this.reservationRequester = null;
    }

    @Autowired
    public FillOrderDispatcher(
            Repository<String, Quote> repository,
            FillSender fillSender,
            RSocketRequester.Builder rsocketRequesterBuilder,
            @Value("${marketmaker.exposure-reservation.host:exposure-reservation}") String reservationHost,
            @Value("${marketmaker.exposure-reservation.port:7000}") int reservationPort) {
        this.quoteRepository = repository;
        this.fillSender = fillSender;
        this.reservationRequester = rsocketRequesterBuilder.tcp(reservationHost, reservationPort);
    }

    @Override
    public void dispatchOrder(ExternalOrder order) {
        logger.info("Dispatching {} order: {} x {} @ {}",
            order.side(), order.symbol(), order.quantity(), order.limitPrice());
        String symbol = order.symbol();
        Object lock = symbolLocks.computeIfAbsent(symbol, k -> new Object());
        synchronized (lock) {
            Quote quote = quoteRepository.get(symbol).orElseThrow(() -> new OrderValidationException("Quote " + symbol + " does not exist"));
            long timestamp = System.currentTimeMillis();
            if (timestamp >= quote.expiresAt()) {
                throw new OrderValidationException("Quote " + symbol + " is expired");
            }
            double price = 0.0;
            switch (order.side()) {
                case BUY:
                    if (order.limitPrice() < quote.askPrice()) {
                        throw new OrderValidationException("Limit price too low to cross ask");
                    } else {
                        price = quote.askPrice();
                    }
                    break;
                case SELL:
                    if (order.limitPrice() > quote.bidPrice()) {
                        throw new OrderValidationException("Limit price too high to cross bid");
                    } else {
                        price = quote.bidPrice();
                    }
            }
            int adjustedQuantity = executableQuantity(quote, order);
            if (adjustedQuantity == 0) {
                throw new OrderValidationException("Order could not be filled");
            }

            Side marketMakerSide = order.side() == Side.BUY ? Side.SELL : Side.BUY;

            releaseReservedExposure(symbol, marketMakerSide, adjustedQuantity);
            applyQuoteFill(quote, order.side(), adjustedQuantity);

            Fill fill = new Fill(order.id(), symbol, marketMakerSide, adjustedQuantity, price, quote.quoteId(), timestamp);
            fillSender.sendFill(fill);
        }
    }

    private int executableQuantity(Quote quote, ExternalOrder order) {
        return Math.min(order.side() == Side.BUY ? quote.askQuantity() : quote.bidQuantity(), order.quantity());
    }

    private void applyQuoteFill(Quote quote, Side side, int amount) {
        quoteRepository.put(updateQuote(quote, side, amount));
    }

    private Quote updateQuote(Quote quote, Side side, int amount) {
        return new Quote(
            quote.symbol(),
            quote.bidPrice(),
            side == Side.SELL ? quote.bidQuantity() - amount : quote.bidQuantity(),
            quote.askPrice(),
            side == Side.BUY ? quote.askQuantity() - amount : quote.askQuantity(),
            quote.quoteId(),
            quote.expiresAt() 
        );
    }

    private void releaseReservedExposure(String symbol, Side side, int quantity) {
        if (reservationRequester == null) {
            logger.debug("Skipping reservation release in test mode: symbol={}, side={}, qty={}", symbol, side, quantity);
            return;
        }

        Fill fillForReservation = new Fill(UUID.randomUUID(), symbol, side, quantity, 0.0, null, System.currentTimeMillis());
        FreedCapacityResponse response = reservationRequester
                .route("reservations." + symbol + ".apply-fill")
                .data(fillForReservation)
                .retrieveMono(FreedCapacityResponse.class)
                .block();

        if (response == null) {
            throw new OrderValidationException("Exposure reservation apply-fill returned no response for " + symbol);
        }

        logger.info("Released reserved exposure: symbol={}, side={}, qty={}, freed={}",
                symbol, side, quantity, response.freedCapacity());
    }
    
}
