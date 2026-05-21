package edu.yu.velocitytrading.exposurereservation;

import edu.yu.velocitytrading.memory.Repository;
import edu.yu.velocitytrading.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Tests for ExposureReservationService
 *
 * From requirements.md:
 *   - Global exposure limit of ±100
 *   - Exposure usage computed from quantities of currently active quotes
 *   - Reservations are granted, reduced, or rejected
 *   - Exposure capacity must be released when quote is filled/replaced/expired
 *   - Capacity must not remain reserved for expired or replaced quotes
 *
 * From components.md:
 *   - Grants, reduces, or rejects exposure reservations
 *   - Updates reservations when a quote is partially or fully filled
 *   - Releases reservations when a quote is replaced or expires
 *   - Ensures capacity is not leaked after crashes, retries, or expirations
 */
class ExposureReservationServiceTest {

    private Repository<String, Reservation> reservationRepository;
    private ExposureReservationService service;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        reservationRepository = mock(Repository.class);
        when(reservationRepository.getAll()).thenReturn(Collections.emptyList());
        when(reservationRepository.get(anyString())).thenReturn(Optional.empty());
        service = new ExposureReservationService(reservationRepository);
    }

    // Grant, full capacity available

    @Test
    void grantsFullyWhenCapacityAvailable() {
        Quote quote = makeQuote("AAPL", 10, 10);

        ReservationResponse response = service.createReservation(quote);

        assertEquals(ReservationStatus.GRANTED, response.status());
        assertEquals(10, response.grantedBidQuantity());
        assertEquals(10, response.grantedAskQuantity());
        assertNotNull(response.id());
        verify(reservationRepository).put(any(Reservation.class));
    }

    @Test
    void grantsFullCapacityOfExactly100() {
        Quote quote = makeQuote("AAPL", 100, 100);

        ReservationResponse response = service.createReservation(quote);

        assertEquals(ReservationStatus.GRANTED, response.status());
        assertEquals(100, response.grantedBidQuantity());
        assertEquals(100, response.grantedAskQuantity());
    }

    @Test
    void grantsZeroQuantityAsGranted() {
        Quote quote = makeQuote("AAPL", 0, 0);

        ReservationResponse response = service.createReservation(quote);

        assertEquals(ReservationStatus.GRANTED, response.status());
        assertEquals(0, response.grantedBidQuantity());
        assertEquals(0, response.grantedAskQuantity());
    }

    // Partial, quote quantity is reduced deterministically to maximum allowed size

    @Test
    void grantsPartiallyWhenCapacityInsufficient() {
        Reservation existing = new Reservation("GOOG", "GOOG", 95, 95, 95, 95, ReservationStatus.GRANTED);
        when(reservationRepository.getAll()).thenReturn(List.of(existing));

        Quote quote = makeQuote("AAPL", 10, 10);
        ReservationResponse response = service.createReservation(quote);

        assertEquals(ReservationStatus.PARTIAL, response.status());
        assertEquals(5, response.grantedBidQuantity(), "Only 5 of 100 remaining");
        assertEquals(5, response.grantedAskQuantity(), "Only 5 of 100 remaining");
    }

    @Test
    void grantsPartiallyWithExactly1UnitRemaining() {
        Reservation existing = new Reservation("GOOG", "GOOG", 99, 99, 99, 99, ReservationStatus.GRANTED);
        when(reservationRepository.getAll()).thenReturn(List.of(existing));

        Quote quote = makeQuote("AAPL", 50, 50);
        ReservationResponse response = service.createReservation(quote);

        assertEquals(ReservationStatus.PARTIAL, response.status());
        assertEquals(1, response.grantedBidQuantity());
        assertEquals(1, response.grantedAskQuantity());
    }

    // Denied, quote not publishe

    @Test
    void deniedWhenNoCapacityRemaining() {
        Reservation existing = new Reservation("GOOG", "GOOG", 100, 100, 100, 100, ReservationStatus.GRANTED);
        when(reservationRepository.getAll()).thenReturn(List.of(existing));

        Quote quote = makeQuote("AAPL", 10, 10);
        ReservationResponse response = service.createReservation(quote);

        assertEquals(ReservationStatus.DENIED, response.status());
        assertEquals(0, response.grantedBidQuantity());
        assertEquals(0, response.grantedAskQuantity());
    }

    @Test
    void deniedWhenOverCapacity() {
        Reservation existing = new Reservation("GOOG", "GOOG", 120, 120, 120, 120, ReservationStatus.GRANTED);
        when(reservationRepository.getAll()).thenReturn(List.of(existing));

        Quote quote = makeQuote("AAPL", 10, 10);
        ReservationResponse response = service.createReservation(quote);

        assertEquals(ReservationStatus.DENIED, response.status());
        assertEquals(0, response.grantedBidQuantity());
        assertEquals(0, response.grantedAskQuantity());
    }

    // Global capacity is shared across all symbols

    @Test
    void multipleSymbolsShareGlobalCapacity() {
        List<Reservation> existing = List.of(
                new Reservation("AAPL", "AAPL", 30, 30, 30, 30, ReservationStatus.GRANTED),
                new Reservation("GOOG", "GOOG", 40, 40, 40, 40, ReservationStatus.GRANTED)
        );
        when(reservationRepository.getAll()).thenReturn(existing);

        Quote quote = makeQuote("MSFT", 50, 50);
        ReservationResponse response = service.createReservation(quote);

        assertEquals(ReservationStatus.PARTIAL, response.status());
        assertEquals(30, response.grantedBidQuantity());
        assertEquals(30, response.grantedAskQuantity());
    }

    @Test
    void releasedReservationsDoNotCountTowardCapacity() {
        List<Reservation> existing = List.of(
                new Reservation("AAPL", "AAPL", 50, 0, 50, 0, ReservationStatus.GRANTED),
                new Reservation("GOOG", "GOOG", 30, 30, 30, 30, ReservationStatus.GRANTED)
        );
        when(reservationRepository.getAll()).thenReturn(existing);

        Quote quote = makeQuote("MSFT", 70, 70);
        ReservationResponse response = service.createReservation(quote);

        assertEquals(ReservationStatus.GRANTED, response.status());
        assertEquals(70, response.grantedBidQuantity());
        assertEquals(70, response.grantedAskQuantity());
    }

    @Test
    void quoteReplacementReleasesOldReservationBeforeGrantingNew() {
        Reservation current = new Reservation("AAPL", "AAPL", 40, 25, 40, 10, ReservationStatus.PARTIAL);
        when(reservationRepository.get("AAPL")).thenReturn(Optional.of(current));

        Quote quote = makeQuote("AAPL", 20, 20);
        service.createReservation(quote);

        ArgumentCaptor<Reservation> captor = ArgumentCaptor.forClass(Reservation.class);
        verify(reservationRepository, times(2)).put(captor.capture());
        List<Reservation> saved = captor.getAllValues();

        assertEquals(0, saved.get(0).grantedBid());
        assertEquals(0, saved.get(0).grantedAsk());
        assertEquals(20, saved.get(1).grantedBid());
        assertEquals(20, saved.get(1).grantedAsk());
    }

    // Apply fill, Exposure reserved: 40, Fill occurs: sell 10, Remaining reserved exposure: 30

    @Test
    void applyFillBuyReducesBidGrant() {
        String symbol = "AAPL";
        Reservation reservation = new Reservation(symbol, symbol, 40, 40, 40, 40, ReservationStatus.GRANTED);
        when(reservationRepository.get(symbol)).thenReturn(Optional.of(reservation));

        int freed = service.applyFill(symbol, 10, Side.BUY);

        assertEquals(10, freed);
        ArgumentCaptor<Reservation> captor = ArgumentCaptor.forClass(Reservation.class);
        verify(reservationRepository).put(captor.capture());
        assertEquals(30, captor.getValue().grantedBid());
        assertEquals(40, captor.getValue().grantedAsk());
    }

    @Test
    void applyFillSellReducesAskGrant() {
        String symbol = "AAPL";
        Reservation reservation = new Reservation(symbol, symbol, 40, 40, 40, 40, ReservationStatus.GRANTED);
        when(reservationRepository.get(symbol)).thenReturn(Optional.of(reservation));

        int freed = service.applyFill(symbol, 15, Side.SELL);

        assertEquals(15, freed);
        ArgumentCaptor<Reservation> captor = ArgumentCaptor.forClass(Reservation.class);
        verify(reservationRepository).put(captor.capture());
        assertEquals(40, captor.getValue().grantedBid());
        assertEquals(25, captor.getValue().grantedAsk());
    }

    @Test
    void applyFillThrowsWhenReservationNotFound() {
        when(reservationRepository.get("AAPL")).thenReturn(Optional.empty());

        assertThrows(RuntimeException.class, () -> service.applyFill("AAPL", 5, Side.BUY));
    }

    // Release, Exposure associated with an expired quote, must be released

    @Test
    void releaseFreesAllRemainingCapacity() {
        String symbol = "AAPL";
        Reservation reservation = new Reservation(symbol, symbol, 30, 30, 30, 30, ReservationStatus.GRANTED);
        when(reservationRepository.get(symbol)).thenReturn(Optional.of(reservation));

        int freed = service.release(symbol);

        assertEquals(60, freed);
        ArgumentCaptor<Reservation> captor = ArgumentCaptor.forClass(Reservation.class);
        verify(reservationRepository).put(captor.capture());
        assertEquals(0, captor.getValue().grantedBid());
        assertEquals(0, captor.getValue().grantedAsk());
    }

    @Test
    void releaseAfterPartialFillFreesRemainder() {
        String symbol = "AAPL";
        Reservation reservation = new Reservation(symbol, symbol, 30, 20, 30, 5, ReservationStatus.PARTIAL);
        when(reservationRepository.get(symbol)).thenReturn(Optional.of(reservation));

        int freed = service.release(symbol);

        assertEquals(25, freed);
    }

    @Test
    void releaseThrowsWhenReservationNotFound() {
        when(reservationRepository.get("AAPL")).thenReturn(Optional.empty());

        assertThrows(RuntimeException.class, () -> service.release("AAPL"));
    }


    // Exposure state, debugging/verification endpoint

    @Test
    void exposureStateReflectsBidAndAskUsage() {
        List<Reservation> reservations = List.of(
                new Reservation("AAPL", "AAPL", 20, 20, 20, 20, ReservationStatus.GRANTED),
                new Reservation("GOOG", "GOOG", 30, 15, 30, 10, ReservationStatus.PARTIAL),
                new Reservation("MSFT", "MSFT", 10, 0, 10, 0, ReservationStatus.DENIED)
        );
        when(reservationRepository.getAll()).thenReturn(reservations);

        ExposureState state = service.getExposureState();

        assertEquals(35, state.bidUsage(), "20 + 15 + 0 = 35");
        assertEquals(30, state.askUsage());
        assertEquals(100, state.totalCapacity());
        assertEquals(2, state.activeReservations(), "Only 2 have granted > 0");
    }

    // Reservation is persisted, state survives restarts

    @Test
    void reservationIsPersistedOnCreate() {
        Quote quote = makeQuote("AAPL", 10, 10);

        service.createReservation(quote);

        ArgumentCaptor<Reservation> captor = ArgumentCaptor.forClass(Reservation.class);
        verify(reservationRepository).put(captor.capture());

        Reservation saved = captor.getValue();
        assertEquals("AAPL", saved.id());
        assertEquals("AAPL", saved.symbol());
        assertEquals(10, saved.requestedBid());
        assertEquals(10, saved.grantedBid());
        assertEquals(10, saved.requestedAsk());
        assertEquals(10, saved.grantedAsk());
        assertEquals(ReservationStatus.GRANTED, saved.status());
    }

    @Test
    void deniedReservationIsAlsoPersisted() {
        Reservation existing = new Reservation("GOOG", "GOOG", 100, 100, 100, 100, ReservationStatus.GRANTED);
        when(reservationRepository.getAll()).thenReturn(List.of(existing));

        Quote quote = makeQuote("AAPL", 10, 10);
        service.createReservation(quote);

        ArgumentCaptor<Reservation> captor = ArgumentCaptor.forClass(Reservation.class);
        verify(reservationRepository).put(captor.capture());
        Reservation saved = captor.getValue();

        assertEquals(0, saved.grantedBid());
        assertEquals(0, saved.grantedAsk());
        assertEquals(ReservationStatus.DENIED, saved.status());
    }


    private Quote makeQuote(String symbol, int bidQty, int askQty) {
        return new Quote(symbol, 99.0, bidQty, 101.0, askQty, UUID.randomUUID(),
                System.currentTimeMillis() + 30_000);
    }
}

