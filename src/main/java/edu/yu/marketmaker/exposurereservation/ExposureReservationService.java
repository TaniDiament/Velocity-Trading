package edu.yu.marketmaker.exposurereservation;

import edu.yu.marketmaker.memory.Repository;
import edu.yu.marketmaker.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Service responsible for managing exposure limits and reservations.
 * Tracks bid and ask exposure independently, each capped at the global limit.
 * Bid exposure represents potential long position increases.
 * Ask exposure represents potential short position decreases.
 */
public class ExposureReservationService {
    private final Logger logger;
    private final Repository<String, Reservation> reservations;

    private static final int MAX_RESERVATION_LIMIT = 100;

    public ExposureReservationService(Repository<String, Reservation> repo) {
        this.reservations = repo;
        this.logger = LoggerFactory.getLogger(ExposureReservationService.class);
    }

    /**
     * Creates a new exposure reservation for both bid and ask sides of a quote.
     * Each side is checked independently against the global limit.
     *
     * @param quote The requested quote containing bid and ask quantities.
     * @return The response containing the reservation ID and the actual granted quantities per side.
     */
    public synchronized ReservationResponse createReservation(Quote quote) {
        logger.info("Creating reservation for Symbol: {}, BidQty: {}, AskQty: {}",
                quote.symbol(), quote.bidQuantity(), quote.askQuantity());

        // Quote replacement: release the old reservation first if this symbol is already active.
        if (reservations.get(quote.symbol()).isPresent()) {
            release(quote.symbol());
        }

        // Each side is capped at MAX_RESERVATION_LIMIT *globally* across all
        // symbols (see class javadoc and unit tests). Summing without a
        // per-symbol filter is required so that aggregate usage never exceeds
        // capacity; getExposureState() also reports the global sum, so the
        // two would otherwise disagree.
        int currentBidUsage = reservations.getAll().stream()
                .mapToInt(Reservation::grantedBid)
                .sum();
        int currentAskUsage = reservations.getAll().stream()
                .mapToInt(Reservation::grantedAsk)
                .sum();

        logger.debug("Current exposure usage: Bid={}/{}, Ask={}/{}",
                currentBidUsage, MAX_RESERVATION_LIMIT, currentAskUsage, MAX_RESERVATION_LIMIT);

        // Calculate available capacity per side
        int availableBid = Math.max(0, MAX_RESERVATION_LIMIT - currentBidUsage);
        int availableAsk = Math.max(0, MAX_RESERVATION_LIMIT - currentAskUsage);

        // Determine requested quantities
        int requestedBid = Math.max(0, quote.bidQuantity());
        int requestedAsk = Math.max(0, quote.askQuantity());

        // Grant only what is available per side, never exceeding the limit
        int grantedBid = Math.min(requestedBid, availableBid);
        int grantedAsk = Math.min(requestedAsk, availableAsk);

        // Determine overall status based on both sides
        ReservationStatus status = determineStatus(requestedBid, grantedBid, requestedAsk, grantedAsk);

        Reservation r = new Reservation(quote.symbol(), quote.symbol(),
                requestedBid, grantedBid, requestedAsk, grantedAsk, status);

        reservations.put(r);

        logger.info("Reservation result: ID={}, Status={}, GrantedBid={}, GrantedAsk={}",
                r.id(), status, grantedBid, grantedAsk);
        return new ReservationResponse(r.id(), r.status(), r.grantedBid(), r.grantedAsk());
    }

    /**
     * Applies a fill to an active reservation on a specific side.
     * When a trade executes, the reserved exposure is converted to actual position,
     * so we free up the reserved capacity corresponding to the fill.
     *
     * @param symbol    The symbol key of the reservation.
     * @param filledQty The quantity that was filled.
     * @param side      The side of the fill (BUY reduces bid exposure, SELL reduces ask exposure).
     * @return The amount of capacity freed by this operation.
     * @throws RuntimeException if the reservation is not found.
     */
    public synchronized int applyFill(String symbol, int filledQty, Side side) {
        logger.info("Applying fill: symbol={}, FilledQty={}, Side={}", symbol, filledQty, side);

        Optional<Reservation> r = reservations.get(symbol);
        if (r.isEmpty()) {
            logger.error("Failed to apply fill: reservation for symbol {} not found", symbol);
            throw new RuntimeException("Reservation not found");
        }

        Reservation reservation = r.get();
        int freed = reduceGrantOnFill(reservation, filledQty, side);

        logger.info("Fill applied successfully: symbol={}, Side={}, FreedCapacity={}", symbol, side, freed);
        return freed;
    }

    /**
     * Releases all remaining capacity for a specific reservation on both sides.
     * This is typically called when a quote is cancelled or expires.
     *
     * @param symbol The symbol key of the reservation.
     * @return The total capacity freed (bid + ask).
     * @throws RuntimeException if the reservation is not found.
     */
    public synchronized int release(String symbol) {
        logger.info("Releasing reservation: symbol={}", symbol);

        Optional<Reservation> r = reservations.get(symbol);
        if (r.isEmpty()) {
            logger.error("Failed to release: reservation for symbol {} not found", symbol);
            throw new RuntimeException("Reservation not found");
        }

        Reservation reservation = r.get();
        int freed = releaseRemaining(reservation);

        logger.info("Reservation released: symbol={}, FreedCapacity={}", symbol, freed);
        return freed;
    }

    /**
     * Reduces the granted (reserved) amount on a specific side based on a fill.
     *
     * @param r       The reservation to modify.
     * @param fillQty The quantity filled.
     * @param side    The side of the fill.
     * @return The amount of capacity freed.
     */
    private int reduceGrantOnFill(Reservation r, int fillQty, Side side) {
        int toFree;
        Reservation updated;

        if (side == Side.BUY) {
            toFree = Math.min(r.grantedBid(), fillQty);
            int newGrantedBid = r.grantedBid() - toFree;
            updated = new Reservation(r.id(), r.symbol(),
                    r.requestedBid(), newGrantedBid, r.requestedAsk(), r.grantedAsk(), r.status());
        } else {
            toFree = Math.min(r.grantedAsk(), fillQty);
            int newGrantedAsk = r.grantedAsk() - toFree;
            updated = new Reservation(r.id(), r.symbol(),
                    r.requestedBid(), r.grantedBid(), r.requestedAsk(), newGrantedAsk, r.status());
        }

        logger.debug("Reducing grant: ID={}, Side={}, Fill={}, Freed={}", r.id(), side, fillQty, toFree);
        reservations.put(updated);
        return toFree;
    }

    /**
     * Sets both granted amounts to zero, effectively releasing the entire reservation.
     *
     * @param r The reservation to release.
     * @return The total amount of capacity freed (bid + ask).
     */
    private int releaseRemaining(Reservation r) {
        int toFree = r.grantedBid() + r.grantedAsk();

        logger.debug("Releasing remaining: ID={}, FreedBid={}, FreedAsk={}, TotalFreed={}",
                r.id(), r.grantedBid(), r.grantedAsk(), toFree);

        Reservation updated = new Reservation(r.id(), r.symbol(),
                r.requestedBid(), 0, r.requestedAsk(), 0, r.status());
        reservations.put(updated);

        return toFree;
    }

    /**
     * Calculates the current system-wide exposure state.
     *
     * @return Snapshot of bid/ask usage, capacity, and active count.
     */
    public ExposureState getExposureState() {
        int bidUsage = reservations.getAll().stream().mapToInt(Reservation::grantedBid).sum();
        int askUsage = reservations.getAll().stream().mapToInt(Reservation::grantedAsk).sum();
        int activeCount = (int) reservations.getAll().stream()
                .filter(r -> r.grantedBid() > 0 || r.grantedAsk() > 0)
                .count();

        logger.debug("Exposure state: BidUsage={}/{}, AskUsage={}/{}, ActiveReservations={}",
                bidUsage, MAX_RESERVATION_LIMIT, askUsage, MAX_RESERVATION_LIMIT, activeCount);
        return new ExposureState(bidUsage, askUsage, MAX_RESERVATION_LIMIT, activeCount);
    }

    /**
     * Determines the overall reservation status based on what was granted vs requested on both sides.
     */
    private ReservationStatus determineStatus(int requestedBid, int grantedBid, int requestedAsk, int grantedAsk) {
        boolean bidFullyGranted = grantedBid >= requestedBid;
        boolean askFullyGranted = grantedAsk >= requestedAsk;
        boolean bidDenied = grantedBid == 0 && requestedBid > 0;
        boolean askDenied = grantedAsk == 0 && requestedAsk > 0;

        if (bidDenied && askDenied) return ReservationStatus.DENIED;
        if (bidFullyGranted && askFullyGranted) return ReservationStatus.GRANTED;
        return ReservationStatus.PARTIAL;
    }
}
