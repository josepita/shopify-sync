from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, DECIMAL, BigInteger, ForeignKey, Enum, Date, Index
from sqlalchemy.orm import relationship
from .connection import Base

class ProductMapping(Base):
    __tablename__ = 'product_mappings'
    
    id = Column(Integer, primary_key=True)
    internal_reference = Column(String(255), unique=True, nullable=False)
    shopify_product_id = Column(BigInteger)
    title = Column(String(255))
    first_created_at = Column(DateTime, default=datetime.utcnow)
    last_updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class VariantMapping(Base):
    __tablename__ = 'variant_mappings'
    
    id = Column(Integer, primary_key=True)
    internal_sku = Column(String(255), unique=True, nullable=False)
    shopify_variant_id = Column(BigInteger)
    shopify_product_id = Column(BigInteger)
    parent_reference = Column(String(255))
    size = Column(String(50))
    price = Column(DECIMAL(10, 2))
    created_at = Column(DateTime, default=datetime.utcnow)
    last_updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    inventory_item_id = Column(BigInteger)

class PriceUpdateQueue(Base):
    __tablename__ = 'price_updates_queue'
    
    id = Column(Integer, primary_key=True)
    variant_mapping_id = Column(Integer, ForeignKey('variant_mappings.id'))
    new_price = Column(DECIMAL(10, 2))
    status = Column(Enum('pending', 'processing', 'completed', 'error'))
    created_at = Column(DateTime, default=datetime.utcnow)
    processed_at = Column(DateTime)

class StockUpdateQueue(Base):
    __tablename__ = 'stock_updates_queue'
    
    id = Column(Integer, primary_key=True)
    variant_mapping_id = Column(Integer, ForeignKey('variant_mappings.id'))
    new_stock = Column(Integer)
    status = Column(Enum('pending', 'processing', 'completed', 'error'))
    created_at = Column(DateTime, default=datetime.utcnow)
    processed_at = Column(DateTime)


class PriceHistory(Base):
    __tablename__ = 'price_history'

    id = Column(Integer, primary_key=True)
    reference = Column(String(255), nullable=False, index=True)
    price = Column(DECIMAL(10, 2), nullable=False)
    date = Column(Date, nullable=False, index=True)

    __table_args__ = (
        Index('price_history_ref_date_idx', 'reference', 'date'),
    )


class StockHistory(Base):
    __tablename__ = 'stock_history'

    id = Column(Integer, primary_key=True)
    reference = Column(String(255), nullable=False, index=True)
    stock = Column(Integer, nullable=False)
    date = Column(Date, nullable=False, index=True)

    __table_args__ = (
        Index('stock_history_ref_date_idx', 'reference', 'date'),
    )
