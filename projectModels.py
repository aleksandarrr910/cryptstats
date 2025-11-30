from sqlalchemy import Column, Integer, Float, Date, String, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

# Ова ќе биде базна класа за сите модели (табели) што ќе ги дефинираме
DNSBase = declarative_base()

# URL за конекција до PostgreSQL базата
SQLUrl = "postgresql://postgres:Slayslatt00*@localhost:5432/dnsDomasnaTable1"


class CryptoCurrency(DNSBase):
    __tablename__ = 'cryptocurrencies'

    id = Column(Integer, primary_key=True, autoincrement=True)
    coinSymbol = Column(String, index=True)
    openTime = Column(Float)
    closeTime = Column(Float)
    high = Column(Float)
    low = Column(Float)
    quoteVolume = Column(Float)
    dateCoin = Column(Date, index=True)
    coinMarketCap = Column(Float)


    def __repr__(self):
        return f"<CryptoCurrency(coinSymbol={self.coinSymbol!r}, id={self.id})>"


def create_dns_engine():

    return create_engine(SQLUrl)


def create_dns_session_factory(engine):

    return sessionmaker(bind=engine)


def init_dns_database(engine):

    DNSBase.metadata.create_all(engine)


workNgn = create_dns_engine()
ConnectionSession = create_dns_session_factory(workNgn)


if __name__ == "__main__":
    init_dns_database(workNgn)
    print("Database and tables are initialized.")
