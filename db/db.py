from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Float,
    DateTime,
    BigInteger,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# remember to use another service to store the credentials
DATABASE_URL = "postgresql://admin:admin@localhost:5432/database"

engine = create_engine(DATABASE_URL)

Base = declarative_base()


# considering I'm saving the data transformed in the consumer
class WeatherData(Base):
    __tablename__ = "weather_data"

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, nullable=False)
    temperature = Column(Float)
    windspeed = Column(Float)
    winddirection = Column(Float)
    weathercode = Column(Integer)


# tracker, review this one
class KafkaOffset(Base):
    __tablename__ = "kafka_offsets"

    id = Column(Integer, primary_key=True)
    topic = Column(String, nullable=False)
    partition = Column(Integer, nullable=False)
    offset = Column(BigInteger, nullable=False)
    timestamp = Column(DateTime, nullable=False)


Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
