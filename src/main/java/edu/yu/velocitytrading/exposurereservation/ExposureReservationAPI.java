package edu.yu.velocitytrading.exposurereservation;

import edu.yu.velocitytrading.model.*;

import edu.yu.velocitytrading.service.ServiceHealth;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.handler.annotation.*;
import org.springframework.web.bind.annotation.*;

/**
 * REST Controller for the Exposure Reservation API.
 * Provides endpoints to reserve exposure capacity, manage fills, and release reservations.
 */
@RestController
@Profile("exposure-reservation")
public class ExposureReservationAPI {

    private final ExposureReservationService service;

    public ExposureReservationAPI(ExposureReservationService service) {
        this.service = service;
    }

    /**
     * POST /reservations
     * Requests exposure capacity for a quote on both bid and ask sides.
     *
     * @param quote The quote containing symbol, bid quantity, and ask quantity.
     * @return A ReservationResponse with granted quantities per side and status (GRANTED, PARTIAL, DENIED).
     */
    @PostMapping("/reservations")
    public ResponseEntity<ReservationResponse> createReservation(@RequestBody Quote quote) {
        return ResponseEntity.ok(service.createReservation(quote));
    }

    /**
     * TCP/RSocket: request-response route {@code "reservations"}.
     * Creates a reservation with the same behavior as POST /reservations.
     */
    @MessageMapping("reservations")
    public ReservationResponse createReservationMessage(@Payload Quote quote) {
        return service.createReservation(quote);
    }

    /**
     * POST /reservations/{symbol}/apply-fill
     * Updates an existing reservation after a partial or full fill occurs.
     * Reduces the reserved exposure on the appropriate side by the filled amount.
     *
     * @param symbol The symbol key of the reservation.
     * @param fill The fill containing quantity and side (BUY or SELL).
     * @return A response indicating the capacity that was freed up by this fill.
     */
    @PostMapping("/reservations/{symbol}/apply-fill")
    public ResponseEntity<FreedCapacityResponse> applyFill(@PathVariable String symbol, @RequestBody Fill fill) {
        int freed = service.applyFill(symbol, fill.quantity(), fill.side());
        return ResponseEntity.ok(new FreedCapacityResponse(freed));
    }

    /**
     * TCP/RSocket: request-response route {@code "reservations.{symbol}.apply-fill"}.
     * Applies a fill with the same behavior as POST /reservations/{symbol}/apply-fill.
     */
    @MessageMapping("reservations.{symbol}.apply-fill")
    public FreedCapacityResponse applyFillMessage(@DestinationVariable String symbol, @Payload Fill fill) {
        int freed = service.applyFill(symbol, fill.quantity(), fill.side());
        return new FreedCapacityResponse(freed);
    }

    /**
     * POST /reservations/{symbol}/release
     * Manually releases a reservation on both sides, typically used when a quote
     * is replaced, canceled, or expires.
     *
     * @param symbol The symbol key of the reservation to release.
     * @return A response indicating the total capacity that was freed.
     */
    @PostMapping("/reservations/{symbol}/release")
    public ResponseEntity<FreedCapacityResponse> release(@PathVariable String symbol) {
        int freed = service.release(symbol);
        return ResponseEntity.ok(new FreedCapacityResponse(freed));
    }

    /**
     * TCP/RSocket: request-response route {@code "reservations.{symbol}.release"}.
     * Releases a reservation with the same behavior as POST /reservations/{symbol}/release.
     */
    @MessageMapping("reservations.{symbol}.release")
    public FreedCapacityResponse releaseMessage(@DestinationVariable String symbol) {
        int freed = service.release(symbol);
        return new FreedCapacityResponse(freed);
    }

    /**
     * GET /exposure
     * Retrieves the current global exposure state with bid and ask usage.
     *
     * @return The current exposure state including bid/ask usage, total capacity, and active reservation count.
     */
    @GetMapping("/exposure")
    public ResponseEntity<ExposureState> getExposure() {
        return ResponseEntity.ok(service.getExposureState());
    }

    /**
     * TCP/RSocket: request-response route {@code "exposure"}.
     * Retrieves exposure with the same behavior as GET /exposure.
     */
    @MessageMapping("exposure")
    public ExposureState getExposureMessage() {
        return service.getExposureState();
    }

    /**
     * GET /health
     * Health check endpoint to verify the service is running and responsive.
     *
     * @return A ServiceHealth object indicating the health status of the service.
     */
    @GetMapping("/health")
    public ResponseEntity<ServiceHealth> getHealth() {
        return ResponseEntity.ok(new ServiceHealth(true, 0, "Exposure Reservation Service"));
    }

    /**
     * TCP/RSocket: request-response route {@code "health"}.
     * Returns service health with the same behavior as GET /health.
     */
    @MessageMapping("health")
    public ServiceHealth getHealthMessage() {
        return new ServiceHealth(true, 0, "Exposure Reservation Service");
    }
}
