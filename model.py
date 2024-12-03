from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import Date, func
from datetime import date, datetime

from database import Base


class SpimexTradingResults(Base):
    __tablename__ = "spimex_trading_results"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, unique=True)
    exchange_product_id: Mapped[str]
    exchange_product_name: Mapped[str]
    oil_id: Mapped[str]
    delivery_basis_id: Mapped[str]
    delivery_basis_name: Mapped[str]
    delivery_type_id: Mapped[str]
    volume: Mapped[int]
    total: Mapped[int]
    count: Mapped[int]
    date: Mapped[date]
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f"SpimexTradingResults(id={self.id!r}, exchange_product_id={self.exchange_product_id!r}, " \
               f"exchange_product_name={self.exchange_product_name!r}, oil_id={self.oil_id!r}, " \
               f"delivery_basis_id={self.delivery_basis_id!r}, delivery_basis_name={self.delivery_basis_name!r}, " \
               f"delivery_type_id={self.delivery_type_id!r}, volume={self.volume!r}, " \
               f"total={self.total!r}, count={self.count!r}, date={self.date!r}, " \
               f"created_at={self.created_at!r}, updated_at={self.updated_at!r})"
