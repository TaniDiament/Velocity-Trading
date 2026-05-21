package edu.yu.velocitytrading.marketmaker;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import edu.yu.velocitytrading.model.StateSnapshot;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

@RestController
@Profile("test-position-tracker")
@RequestMapping("/test/position-tracker")
public class TestPositionTracker implements SnapshotTracker {

	private final Set<String> trackedSymbols = ConcurrentHashMap.newKeySet();
	private final Set<String> receivedSymbols = ConcurrentHashMap.newKeySet();
	private final Sinks.Many<StateSnapshot> snapshotSink =
			Sinks.many().multicast().onBackpressureBuffer();

	@Override
	public Flux<StateSnapshot> getPositions() {
		return snapshotSink.asFlux()
				.filter(snapshot -> snapshot.position() != null
						&& trackedSymbols.contains(snapshot.position().symbol()));
	}

	@Override
	public boolean addSymbol(String symbol) {
		return trackedSymbols.add(symbol);
	}

	@Override
	public boolean removeSymbol(String symbol) {
		return trackedSymbols.remove(symbol);
	}

	@Override
	public boolean handlesSymbol(String symbol) {
		return trackedSymbols.contains(symbol);
	}

	@PutMapping("/symbols/{symbol}")
	public ResponseEntity<Void> addTrackedSymbol(@PathVariable String symbol) {
		if (symbol == null || symbol.isBlank()) {
			return ResponseEntity.badRequest().build();
		}
		trackedSymbols.add(symbol);
		return ResponseEntity.noContent().build();
	}

	@GetMapping("/received/{symbol}")
	public ResponseEntity<Boolean> hasReceivedSymbol(@PathVariable String symbol) {
		if (symbol == null || symbol.isBlank()) {
			return ResponseEntity.badRequest().build();
		}
		return ResponseEntity.ok(receivedSymbols.contains(symbol));
	}

	@PostMapping("/snapshots")
	public ResponseEntity<Void> submitSnapshot(@RequestBody StateSnapshot snapshot) {
		if (snapshot == null || snapshot.position() == null || snapshot.position().symbol() == null) {
			return ResponseEntity.badRequest().build();
		}
		Sinks.EmitResult result = snapshotSink.tryEmitNext(snapshot);
		if (result.isFailure()) {
			return ResponseEntity.internalServerError().build();
		}
		receivedSymbols.add(snapshot.position().symbol());
		return ResponseEntity.accepted().build();
	}
}
