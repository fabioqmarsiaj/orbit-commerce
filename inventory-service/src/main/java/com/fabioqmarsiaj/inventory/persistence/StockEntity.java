package com.fabioqmarsiaj.inventory.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import org.springframework.data.domain.Persistable;

import java.time.Instant;

/**
 * JPA entity for the current stock level of a single product.
 *
 * <p>Unlike {@code order-service}'s entities, the id here ({@code productId})
 * is a natural/business key supplied by the caller of the seed endpoint
 * (e.g. {@code "sku-123"}), not a randomly generated {@link java.util.UUID}.
 * It's still always non-null the moment the entity is constructed though,
 * which trips up Spring Data JPA's default "is the @Id null?" heuristic for
 * deciding {@code persist()} (INSERT) vs {@code merge()} (UPDATE) in exactly
 * the same way app-assigned UUIDs did for {@code order-service}'s entities
 * — see {@code docs/decisions.md} for the full writeup. So this entity
 * implements {@link Persistable} too, using the same
 * "{@code @Transient} flag set only by the brand-new-row constructor"
 * technique as {@code order-service}'s {@code OutboxEntity}.
 *
 * <p>Note that the hot path (reserving/releasing stock, in
 * {@code StockService}) does NOT go through this entity's {@code isNew()}
 * at all — it uses a direct {@code @Modifying} UPDATE query
 * (see {@link StockRepository#tryReserve}) for atomicity, bypassing the
 * JPA entity lifecycle entirely. {@code isNew()} only matters for the
 * seed endpoint's upsert-by-{@code save()} call.
 */
@Entity
@Table(name = "stock")
public class StockEntity implements Persistable<String> {

    @Id
    @Column(name = "product_id", length = 100)
    private String productId;

    @Column(name = "quantity_available", nullable = false)
    private int quantityAvailable;

    @Column(name = "quantity_reserved", nullable = false)
    private int quantityReserved;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    /** See class javadoc for why this exists and how it's kept correct. */
    @Transient
    private boolean isNew = false;

    protected StockEntity() {
        // required by JPA
    }

    /** Used by the seed endpoint to create a brand-new product row. */
    public StockEntity(String productId, int quantityAvailable) {
        this.productId = productId;
        this.quantityAvailable = quantityAvailable;
        this.quantityReserved = 0;
        this.updatedAt = Instant.now();
        this.isNew = true;
    }

    @Override
    public String getId() {
        return productId;
    }

    @Override
    public boolean isNew() {
        return isNew;
    }

    public String getProductId() {
        return productId;
    }

    public int getQuantityAvailable() {
        return quantityAvailable;
    }

    public int getQuantityReserved() {
        return quantityReserved;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    /** Used by the seed endpoint when re-seeding an already-existing product. */
    public void reseed(int quantityAvailable) {
        this.quantityAvailable = quantityAvailable;
        this.updatedAt = Instant.now();
    }
}
