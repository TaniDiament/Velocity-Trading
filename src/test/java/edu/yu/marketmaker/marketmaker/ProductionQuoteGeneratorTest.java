package edu.yu.marketmaker.marketmaker;

import edu.yu.marketmaker.ha.LeaderAwareRSocketClient;
import edu.yu.marketmaker.memory.Repository;
import edu.yu.marketmaker.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.ObjectProvider;
import reactor.core.publisher.Mono;

import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for ProductionQuoteGenerator
 *
 * From components.md:
 *   - Generates bid/ask quotes using inventory-aware pricing
 *   - Skews price and quantity based on the last fill side
 *   - Enforces individual position limits of ±100
 *   - Calls ExposureReservation service and uses granted quantities
 *   - Persists the reserved quote for use as the next reference price
 */
class ProductionQuoteGeneratorTest {

    private Repository<String, Quote> quoteRepository;
    private LeaderAwareRSocketClient rsocketClient;
    private ProductionQuoteGenerator generator;

    private static final int DEFAULT_QTY = 10;
    private static final double TARGET_SPREAD = 0.10;
    private static final double HALF_SPREAD = 0.05;
    private static final String EXPOSURE_RESERVATION_SERVICE = "exposure-reservation";

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        quoteRepository = mock(Repository.class);
        rsocketClient = mock(LeaderAwareRSocketClient.class);
        when(quoteRepository.get(anyString())).thenReturn(Optional.empty());

        @SuppressWarnings("unchecked")
        ObjectProvider<FaultInjector> emptyProvider = mock(ObjectProvider.class);
        when(emptyProvider.getIfAvailable()).thenReturn(null);
        generator = new ProductionQuoteGenerator(rsocketClient, quoteRepository, DEFAULT_QTY, TARGET_SPREAD, emptyProvider);
    }

    private void stubReservation(int grantedBid, int grantedAsk) {
        ReservationResponse response = new ReservationResponse(
                UUID.randomUUID().toString(), ReservationStatus.GRANTED, grantedBid, grantedAsk
        );
        when(rsocketClient.requestResponse(eq(EXPOSURE_RESERVATION_SERVICE), eq("reservations"),
                any(), eq(ReservationResponse.class))).thenReturn(Mono.just(response));
    }

    private ArgumentCaptor<Quote> captureProposed() {
        ArgumentCaptor<Quote> captor = ArgumentCaptor.forClass(Quote.class);
        verify(rsocketClient).requestResponse(eq(EXPOSURE_RESERVATION_SERVICE), eq("reservations"),
                captor.capture(), eq(ReservationResponse.class));
        return captor;
    }

    private Position flatPosition(String symbol) {
        return new Position(symbol, 0, 1L, null);
    }

    private Fill makeFill(String symbol, Side side, int qty, double price) {
        return new Fill(UUID.randomUUID(), symbol, side, qty, price, UUID.randomUUID(), System.currentTimeMillis());
    }

    // --- No prior state ---

    /**
     * When no current quote exists and no fill is provided, the generator has no market context.
     * It should fall back to the hardcoded reference price of 100.0 and the configured default
     * quantity, then apply the target spread symmetrically to produce bid/ask prices.
     */
    @Test
    void usesDefaultPriceAndQuantityWhenNoPriorState() {
        stubReservation(DEFAULT_QTY, DEFAULT_QTY);
        Position pos = flatPosition("AAPL");

        Quote quote = generator.generateQuote(pos, null);

        assertEquals("AAPL", quote.symbol());
        assertEquals(100.0 - HALF_SPREAD, quote.bidPrice(), 1e-9);
        assertEquals(100.0 + HALF_SPREAD, quote.askPrice(), 1e-9);
        assertEquals(DEFAULT_QTY, quote.bidQuantity());
        assertEquals(DEFAULT_QTY, quote.askQuantity());
    }

    /**
     * When there is no current quote in the repository but a fill is provided, the fill's
     * execution price is the only available market anchor. The generator should use that price
     * as the reference instead of the hardcoded 100.0 default, then apply skew on top of it.
     */
    @Test
    void usesFillPriceAsReferenceWhenNoCurrentQuote() {
        double fillPrice = 150.0;
        int fillQty = 5;
        stubReservation(DEFAULT_QTY + 2, DEFAULT_QTY - 1);
        Position pos = flatPosition("AAPL");
        Fill fill = makeFill("AAPL", Side.BUY, fillQty, fillPrice);

        ArgumentCaptor<Quote> captor = ArgumentCaptor.forClass(Quote.class);
        generator.generateQuote(pos, fill);

        captor = captureProposed();
        // After BUY fill: referencePrice = fillPrice - 0.01 * qty
        double expectedRef = fillPrice - 0.01 * fillQty;
        assertEquals(expectedRef - HALF_SPREAD, captor.getValue().bidPrice(), 1e-9);
        assertEquals(expectedRef + HALF_SPREAD, captor.getValue().askPrice(), 1e-9);
    }

    // --- Inventory-aware skew: SELL fill ---

    /**
     * A SELL fill means shares left the inventory, so the market maker wants to attract buyers
     * to replenish stock. It raises the reference price by 0.01 per unit sold, making both
     * bid and ask prices higher and signaling willingness to buy back at a better price.
     */
    @Test
    void sellFillRaisesReferencePriceByFillQuantity() {
        double fillPrice = 100.0;
        int fillQty = 5;
        stubReservation(DEFAULT_QTY - 1, DEFAULT_QTY + 2);
        Position pos = flatPosition("AAPL");
        Fill fill = makeFill("AAPL", Side.SELL, fillQty, fillPrice);

        ArgumentCaptor<Quote> captor = ArgumentCaptor.forClass(Quote.class);
        generator.generateQuote(pos, fill);

        captor = captureProposed();
        double expectedRef = fillPrice + 0.01 * fillQty;
        assertEquals(expectedRef - HALF_SPREAD, captor.getValue().bidPrice(), 1e-9);
        assertEquals(expectedRef + HALF_SPREAD, captor.getValue().askPrice(), 1e-9);
    }

    /**
     * After a SELL fill the market maker is short inventory, so it increases ask size by 2
     * (still willing to sell more at the higher price) and decreases bid size by 1
     * (less eager to buy given the current short position).
     */
    @Test
    void sellFillIncreasesAskQuantityByTwoAndDecreasesBidByOne() {
        stubReservation(DEFAULT_QTY - 1, DEFAULT_QTY + 2);
        Position pos = flatPosition("AAPL");
        Fill fill = makeFill("AAPL", Side.SELL, 5, 100.0);

        ArgumentCaptor<Quote> captor = ArgumentCaptor.forClass(Quote.class);
        generator.generateQuote(pos, fill);

        captor = captureProposed();
        assertEquals(DEFAULT_QTY - 1, captor.getValue().bidQuantity(), "bidQty should decrease by 1 after SELL");
        assertEquals(DEFAULT_QTY + 2, captor.getValue().askQuantity(), "askQty should increase by 2 after SELL");
    }

    /**
     * When the existing bid quantity is already at 1, subtracting 1 for a SELL fill skew
     * would produce a negative bid quantity. The generator must clamp to 0 rather than
     * publishing a quote with a negative quantity.
     */
    @Test
    void sellFillWithSingleBidQuantityDoesNotGoBelowZero() {
        Quote current = new Quote("AAPL", 99.0, 1, 101.0, 5, UUID.randomUUID(), System.currentTimeMillis() + 30_000);
        when(quoteRepository.get("AAPL")).thenReturn(Optional.of(current));
        stubReservation(0, 7);
        Position pos = flatPosition("AAPL");
        Fill fill = makeFill("AAPL", Side.SELL, 3, 100.0);

        ArgumentCaptor<Quote> captor = ArgumentCaptor.forClass(Quote.class);
        generator.generateQuote(pos, fill);

        captor = captureProposed();
        assertEquals(0, captor.getValue().bidQuantity(), "bidQty should not go below zero");
    }

    // --- Inventory-aware skew: BUY fill ---

    /**
     * A BUY fill means the market maker accumulated inventory, so it lowers the reference
     * price by 0.01 per unit bought to attract sellers and offload the position. Both bid
     * and ask prices shift down accordingly.
     */
    @Test
    void buyFillLowersReferencePriceByFillQuantity() {
        double fillPrice = 100.0;
        int fillQty = 10;
        stubReservation(DEFAULT_QTY + 2, DEFAULT_QTY - 1);
        Position pos = flatPosition("AAPL");
        Fill fill = makeFill("AAPL", Side.BUY, fillQty, fillPrice);

        ArgumentCaptor<Quote> captor = ArgumentCaptor.forClass(Quote.class);
        generator.generateQuote(pos, fill);

        captor = captureProposed();
        double expectedRef = fillPrice - 0.01 * fillQty;
        assertEquals(expectedRef - HALF_SPREAD, captor.getValue().bidPrice(), 1e-9);
        assertEquals(expectedRef + HALF_SPREAD, captor.getValue().askPrice(), 1e-9);
    }

    /**
     * After a BUY fill the market maker is long inventory, so it increases bid size by 2
     * (still willing to buy more to average in) and decreases ask size by 1 (less eager
     * to sell given the long position that needs to be offloaded at better prices).
     */
    @Test
    void buyFillIncreasesBidQuantityByTwoAndDecreasesAskByOne() {
        stubReservation(DEFAULT_QTY + 2, DEFAULT_QTY - 1);
        Position pos = flatPosition("AAPL");
        Fill fill = makeFill("AAPL", Side.BUY, 5, 100.0);

        ArgumentCaptor<Quote> captor = ArgumentCaptor.forClass(Quote.class);
        generator.generateQuote(pos, fill);

        captor = captureProposed();
        assertEquals(DEFAULT_QTY + 2, captor.getValue().bidQuantity(), "bidQty should increase by 2 after BUY");
        assertEquals(DEFAULT_QTY - 1, captor.getValue().askQuantity(), "askQty should decrease by 1 after BUY");
    }

    /**
     * When the existing ask quantity is already at 1, subtracting 1 for a BUY fill skew
     * would produce a negative ask quantity. The generator must clamp to 0 rather than
     * publishing a quote with a negative quantity.
     */
    @Test
    void buyFillWithSingleAskQuantityDoesNotGoBelowZero() {
        Quote current = new Quote("AAPL", 99.0, 5, 101.0, 1, UUID.randomUUID(), System.currentTimeMillis() + 30_000);
        when(quoteRepository.get("AAPL")).thenReturn(Optional.of(current));
        stubReservation(7, 0);
        Position pos = flatPosition("AAPL");
        Fill fill = makeFill("AAPL", Side.BUY, 3, 100.0);

        ArgumentCaptor<Quote> captor = ArgumentCaptor.forClass(Quote.class);
        generator.generateQuote(pos, fill);

        captor = captureProposed();
        assertEquals(0, captor.getValue().askQuantity(), "askQty should not go below zero");
    }

    // --- Existing quote used as reference ---

    /**
     * When a previous quote exists in the repository, its mid-price (average of bid and ask)
     * should be used as the reference price for the next quote. This provides continuity and
     * prevents sudden price jumps between quote refreshes when no fill has occurred.
     */
    @Test
    void usesMidPriceOfExistingQuoteAsReference() {
        Quote current = new Quote("AAPL", 99.0, 10, 101.0, 10, UUID.randomUUID(), System.currentTimeMillis() + 30_000);
        when(quoteRepository.get("AAPL")).thenReturn(Optional.of(current));
        stubReservation(DEFAULT_QTY, DEFAULT_QTY);
        Position pos = flatPosition("AAPL");

        ArgumentCaptor<Quote> captor = ArgumentCaptor.forClass(Quote.class);
        generator.generateQuote(pos, null);

        captor = captureProposed();
        double midPrice = (99.0 + 101.0) / 2.0;
        assertEquals(midPrice - HALF_SPREAD, captor.getValue().bidPrice(), 1e-9);
        assertEquals(midPrice + HALF_SPREAD, captor.getValue().askPrice(), 1e-9);
    }

    /**
     * When a previous quote exists and no fill has occurred, the bid and ask quantities from
     * that quote should be carried forward as the starting point for the new quote. This
     * maintains size continuity rather than resetting to the default quantity on every refresh.
     */
    @Test
    void carriesOverQuantitiesFromExistingQuote() {
        Quote current = new Quote("AAPL", 99.0, 7, 101.0, 8, UUID.randomUUID(), System.currentTimeMillis() + 30_000);
        when(quoteRepository.get("AAPL")).thenReturn(Optional.of(current));
        stubReservation(7, 8);
        Position pos = flatPosition("AAPL");

        ArgumentCaptor<Quote> captor = ArgumentCaptor.forClass(Quote.class);
        generator.generateQuote(pos, null);

        captor = captureProposed();
        assertEquals(7, captor.getValue().bidQuantity());
        assertEquals(8, captor.getValue().askQuantity());
    }

    // --- Position limits ---

    /**
     * When the net position is close to the +100 long limit, the maximum additional shares
     * we can buy is (100 - netQuantity). The proposed bid quantity must be capped at this
     * value to prevent exceeding the individual position limit.
     */
    @Test
    void positionLimitCapsMaxBidQuantity() {
        stubReservation(5, DEFAULT_QTY);
        Position pos = new Position("AAPL", 95, 1L, null);

        ArgumentCaptor<Quote> captor = ArgumentCaptor.forClass(Quote.class);
        generator.generateQuote(pos, null);

        captor = captureProposed();
        assertEquals(5, captor.getValue().bidQuantity(), "bidQty capped at 100 - 95 = 5");
    }

    /**
     * When the net position is close to the -100 short limit, the maximum additional shares
     * we can sell is (100 + netQuantity). The proposed ask quantity must be capped at this
     * value to prevent exceeding the individual position limit on the short side.
     */
    @Test
    void positionLimitCapsMaxAskQuantity() {
        stubReservation(DEFAULT_QTY, 5);
        Position pos = new Position("AAPL", -95, 1L, null);

        ArgumentCaptor<Quote> captor = ArgumentCaptor.forClass(Quote.class);
        generator.generateQuote(pos, null);

        captor = captureProposed();
        assertEquals(5, captor.getValue().askQuantity(), "askQty capped at 100 + (-95) = 5");
    }

    /**
     * When the net position is exactly at the maximum long limit of +100, maxAllowedBid
     * computes to zero. No additional buying is allowed, so the bid quantity must be 0
     * regardless of skew or default quantity.
     */
    @Test
    void positionAtMaxLongSetsBidQuantityToZero() {
        stubReservation(0, DEFAULT_QTY);
        Position pos = new Position("AAPL", 100, 1L, null);

        ArgumentCaptor<Quote> captor = ArgumentCaptor.forClass(Quote.class);
        generator.generateQuote(pos, null);

        captor = captureProposed();
        assertEquals(0, captor.getValue().bidQuantity(), "cannot buy more when at max long position");
    }

    /**
     * When the net position is exactly at the maximum short limit of -100, maxAllowedAsk
     * computes to zero. No additional selling is allowed, so the ask quantity must be 0
     * regardless of skew or default quantity.
     */
    @Test
    void positionAtMaxShortSetsAskQuantityToZero() {
        stubReservation(DEFAULT_QTY, 0);
        Position pos = new Position("AAPL", -100, 1L, null);

        ArgumentCaptor<Quote> captor = ArgumentCaptor.forClass(Quote.class);
        generator.generateQuote(pos, null);

        captor = captureProposed();
        assertEquals(0, captor.getValue().askQuantity(), "cannot sell more when at max short position");
    }

    /**
     * The position limit check occurs after inventory skew is applied. A BUY fill skew would
     * normally raise the bid quantity by 2 (to 12), but if the position is near the long limit
     * the cap must take precedence. This verifies the ordering: skew first, then clamp.
     */
    @Test
    void positionLimitIsAppliedAfterSkew() {
        // Net +98, max allowed bid = 2. After BUY fill bidQty would be 12, but position cap overrides.
        stubReservation(2, DEFAULT_QTY - 1);
        Position pos = new Position("AAPL", 98, 1L, null);
        Fill fill = makeFill("AAPL", Side.BUY, 5, 100.0);

        ArgumentCaptor<Quote> captor = ArgumentCaptor.forClass(Quote.class);
        generator.generateQuote(pos, fill);

        captor = captureProposed();
        assertEquals(2, captor.getValue().bidQuantity(), "position cap of 2 overrides skew-adjusted qty of 12");
    }

    // --- Reservation response applied ---

    /**
     * The exposure reservation service may grant fewer bid shares than requested if global
     * capacity is limited. The returned quote must use the granted bid quantity, not the
     * proposed quantity, so that the published quote never over-commits exposure.
     */
    @Test
    void returnedQuoteUsesGrantedBidQuantityFromReservation() {
        stubReservation(7, DEFAULT_QTY);
        Position pos = flatPosition("AAPL");

        Quote quote = generator.generateQuote(pos, null);

        assertEquals(7, quote.bidQuantity(), "returned quote must reflect granted bid from reservation");
    }

    /**
     * The exposure reservation service may grant fewer ask shares than requested if global
     * capacity is limited. The returned quote must use the granted ask quantity, not the
     * proposed quantity, so that the published quote never over-commits exposure.
     */
    @Test
    void returnedQuoteUsesGrantedAskQuantityFromReservation() {
        stubReservation(DEFAULT_QTY, 3);
        Position pos = flatPosition("AAPL");

        Quote quote = generator.generateQuote(pos, null);

        assertEquals(3, quote.askQuantity(), "returned quote must reflect granted ask from reservation");
    }

    /**
     * If the reservation service returns an empty response (e.g. a network or serialization
     * failure), continuing without a reservation would silently over-commit exposure. The
     * generator must throw an IllegalStateException to surface the failure immediately.
     */
    @Test
    void throwsWhenReservationServiceReturnsNull() {
        when(rsocketClient.requestResponse(eq(EXPOSURE_RESERVATION_SERVICE), eq("reservations"),
                any(), eq(ReservationResponse.class))).thenReturn(Mono.empty());
        Position pos = flatPosition("AAPL");

        assertThrows(IllegalStateException.class, () -> generator.generateQuote(pos, null));
    }

    // --- Spread ---

    /**
     * The target spread is a configurable parameter representing the total distance between
     * bid and ask. It must be split evenly (half on each side of the reference price), so
     * the difference between askPrice and bidPrice in the proposed quote equals TARGET_SPREAD.
     */
    @Test
    void spreadIsAppliedSymmetricallyAroundReferencePrice() {
        stubReservation(DEFAULT_QTY, DEFAULT_QTY);
        Position pos = flatPosition("AAPL");

        ArgumentCaptor<Quote> captor = ArgumentCaptor.forClass(Quote.class);
        generator.generateQuote(pos, null);

        captor = captureProposed();
        assertEquals(TARGET_SPREAD, captor.getValue().askPrice() - captor.getValue().bidPrice(), 1e-9);
    }

    // --- Persistence ---

    /**
     * After the reservation is obtained, the final quote (with reservation-granted quantities)
     * must be saved to the repository. This persisted quote becomes the reference price and
     * carry-over quantities for the next call to generateQuote for the same symbol.
     */
    @Test
    void persistsReservedQuoteToRepository() {
        stubReservation(7, 3);
        Position pos = flatPosition("AAPL");

        generator.generateQuote(pos, null);

        ArgumentCaptor<Quote> captor = ArgumentCaptor.forClass(Quote.class);
        verify(quoteRepository).put(captor.capture());
        assertEquals("AAPL", captor.getValue().symbol());
        assertEquals(7, captor.getValue().bidQuantity());
        assertEquals(3, captor.getValue().askQuantity());
    }

    /**
     * The reservation service only adjusts quantities, not prices. The persisted quote must
     * retain the bid and ask prices computed from the reference price and spread, even when
     * the reservation reduces the quantities from what was originally proposed.
     */
    @Test
    void persistedQuotePreservesPricesFromProposedQuote() {
        stubReservation(7, 3);
        Position pos = flatPosition("AAPL");

        generator.generateQuote(pos, null);

        ArgumentCaptor<Quote> captor = ArgumentCaptor.forClass(Quote.class);
        verify(quoteRepository).put(captor.capture());
        assertEquals(100.0 - HALF_SPREAD, captor.getValue().bidPrice(), 1e-9);
        assertEquals(100.0 + HALF_SPREAD, captor.getValue().askPrice(), 1e-9);
    }

    // --- Symbol resolution ---

    /**
     * When a fill is present, the symbol on the fill identifies which instrument was traded
     * and should drive the resulting quote. The position symbol is used only as a fallback,
     * so the fill's symbol must take precedence.
     */
    @Test
    void usesSymbolFromLastFillWhenPresent() {
        stubReservation(DEFAULT_QTY, DEFAULT_QTY);
        Position pos = new Position("OTHER", 0, 1L, null);
        Fill fill = makeFill("AAPL", Side.BUY, 5, 100.0);

        Quote quote = generator.generateQuote(pos, fill);

        assertEquals("AAPL", quote.symbol(), "symbol should come from lastFill when present");
    }

    /**
     * When no fill is provided there is no fill symbol to use, so the quote symbol must
     * fall back to the position's symbol. This is the standard path for a periodic
     * quote refresh triggered by a position update with no accompanying trade.
     */
    @Test
    void usesSymbolFromPositionWhenNoFill() {
        stubReservation(DEFAULT_QTY, DEFAULT_QTY);
        Position pos = flatPosition("MSFT");

        Quote quote = generator.generateQuote(pos, null);

        assertEquals("MSFT", quote.symbol());
    }
}
