package edu.yu.marketmaker.persistence;

import edu.yu.marketmaker.model.Quote;
import jakarta.persistence.*;

import java.util.UUID;

/**
 * JPA Entity for Quote records used by Hazelcast MapStore.
 */
@Entity
@Table(name = "quotes")
public class QuoteEntity implements IdentifiableEntity<String> {

    @Id
    private String symbol;
    private UUID quoteId;
    private double bidPrice;
    private int bidQuantity;
    private double askPrice;
    private int askQuantity;
    private long expiresAt;

    // --- Constructors ---

    /**
     * No-args constructor required by JPA.
     */
    public QuoteEntity() {
    }

    /**
     * All-args constructor.
     */
    public QuoteEntity(UUID quoteId, String symbol, double bidPrice, int bidQuantity, double askPrice, int askQuantity, long expiresAt) {
        this.quoteId = quoteId;
        this.symbol = symbol;
        this.bidPrice = bidPrice;
        this.bidQuantity = bidQuantity;
        this.askPrice = askPrice;
        this.askQuantity = askQuantity;
        this.expiresAt = expiresAt;
    }

    // --- IdentifiableEntity Implementation ---

    @Override
    public String getId() {
        return symbol;
    }

    // --- Conversion Methods ---

    /**
     * Converts this JPA entity back into the immutable Quote record.
     * @return A Quote record.
     */
    public Quote toRecord() {
        return new Quote(this.symbol, this.bidPrice, this.bidQuantity, this.askPrice, this.askQuantity, this.quoteId, this.expiresAt);
    }

    /**
     * Static helper to create an Entity from a Record.
     * @param quote The quote record.
     * @return A new QuoteEntity.
     */
    public static QuoteEntity fromRecord(Quote quote) {
        return new QuoteEntity(
                quote.quoteId(),
                quote.symbol(),
                quote.bidPrice(),
                quote.bidQuantity(),
                quote.askPrice(),
                quote.askQuantity(),
                quote.expiresAt()
        );
    }

    // --- Getters and Setters ---

    public UUID getQuoteId() {
        return quoteId;
    }

    public void setQuoteId(UUID quoteId) {
        this.quoteId = quoteId;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public double getBidPrice() {
        return bidPrice;
    }

    public void setBidPrice(double bidPrice) {
        this.bidPrice = bidPrice;
    }

    public int getBidQuantity() {
        return bidQuantity;
    }

    public void setBidQuantity(int bidQuantity) {
        this.bidQuantity = bidQuantity;
    }

    public double getAskPrice() {
        return askPrice;
    }

    public void setAskPrice(double askPrice) {
        this.askPrice = askPrice;
    }

    public int getAskQuantity() {
        return askQuantity;
    }

    public void setAskQuantity(int askQuantity) {
        this.askQuantity = askQuantity;
    }

    public long getExpiresAt() {
        return expiresAt;
    }

    public void setExpiresAt(long expiresAt) {
        this.expiresAt = expiresAt;
    }

    @Override
    public String toString() {
        return "QuoteEntity{" +
                "quoteId=" + quoteId +
                ", symbol='" + symbol + '\'' +
                ", bidPrice=" + bidPrice +
                ", bidQuantity=" + bidQuantity +
                ", askPrice=" + askPrice +
                ", askQuantity=" + askQuantity +
                ", expiresAt=" + expiresAt +
                '}';
    }
}
