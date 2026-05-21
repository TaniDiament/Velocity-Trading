package edu.yu.velocitytrading.persistence;

import edu.yu.velocitytrading.model.Position;
import jakarta.persistence.*;
import java.util.UUID;

/**
 * Entity used by hazelcast
 */
@Entity
@Table(name = "positions")
public class PositionEntity implements IdentifiableEntity<String> {

    @Id
    private String symbol;
    private int netQuantity;
    private long version;
    private UUID lastFillId;

    // --- Constructors ---

    /**
     * No-args constructor required by JPA.
     */
    public PositionEntity() {
    }

    /**
     * All-args constructor.
     */
    public PositionEntity(String symbol, int netQuantity, long version, UUID lastFillId) {
        this.symbol = symbol;
        this.netQuantity = netQuantity;
        this.version = version;
        this.lastFillId = lastFillId;
    }

    // --- IdentifiableEntity Implementation ---

    @Override
    public String getId() {
        return symbol;
    }

    // --- Conversion Methods ---

    /**
     * Converts this JPA entity back into the immutable Position record used by the application.
     * @return A Position record.
     */
    public Position toRecord() {
        return new Position(this.symbol, this.netQuantity, this.version, this.lastFillId);
    }

    /**
     * Static helper to create an Entity from a Record.
     * Useful for the MapStore.store() method.
     * @param position The position record.
     * @return A new PositionEntity.
     */
    public static PositionEntity fromRecord(Position position) {
        return new PositionEntity(
                position.symbol(),
                position.netQuantity(),
                position.version(),
                position.lastFillId()
        );
    }

    // --- Getters and Setters ---

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public int getNetQuantity() {
        return netQuantity;
    }

    public void setNetQuantity(int netQuantity) {
        this.netQuantity = netQuantity;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public UUID getLastFillId() {
        return lastFillId;
    }

    public void setLastFillId(UUID lastFillId) {
        this.lastFillId = lastFillId;
    }

    @Override
    public String toString() {
        return "PositionEntity{" +
                "symbol='" + symbol + '\'' +
                ", netQuantity=" + netQuantity +
                ", version=" + version +
                ", lastFillId=" + lastFillId +
                '}';
    }
}