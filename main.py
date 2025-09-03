import os
import logging
from datetime import datetime
import pytz
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, Text, select, delete, Float
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

# ---------------- LOAD ENV AND SETUP LOGGING ----------------
load_dotenv()
# Default to a local SQLite DB if DATABASE_URL is not set, for portability
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./relative_strength.db")
if not DATABASE_URL:
    raise ValueError("❌ Please set DATABASE_URL in .env file or ensure the script can create a local sqlite db.")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")

def now_ist():
    """Returns the current time in IST timezone."""
    return datetime.now(IST)

# ---------------- SQLALCHEMY SETUP ----------------
try:
    engine = create_engine(DATABASE_URL, echo=False)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base = declarative_base()
except Exception as e:
    logger.critical(f"Failed to connect to the database: {e}")
    exit(1)


def get_db_session():
    """Provides a transactional scope around a series of operations."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def create_tables():
    """Create all tables in the metadata if they do not exist."""
    logger.info("Initializing database tables...")
    Base.metadata.create_all(bind=engine)
    logger.info("Table initialization complete.")


# ---------------- ORM TABLE ----------------
class SgRelativeStrength(Base):
    __tablename__ = "relative_strength_info"

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    symbol = Column("Symbol", String(50), nullable=False, unique=True)
    ltp = Column("LTP", String(50))
    price_percentage = Column("Price Percentage", String(50), nullable=True)
    relstrength_value = Column("RELSTRENGTH VALUE", String(50))
    relstrength = Column("RELSTRENGTH", String(50))
    created_at = Column(String(100), default=lambda: now_ist().isoformat())

    def __repr__(self):
        return f"<SgRelativeStrength(symbol='{self.symbol}', ltp='{self.ltp}', price_percentage='{self.price_percentage}', relstrength='{self.relstrength}', created_at='{self.created_at}')>"


# ---------------- REPOSITORY ----------------
class SgRelativeStrengthRepository:
    """
    Handles all database operations for the SgRelativeStrength table.
    """
    def __init__(self, db_session: Session):
        self.session = db_session

    def upsert(self, record_data: dict):
        """
        Inserts or updates a single record, handling special parsing and append logic.
        The caller is responsible for transaction management (commit/rollback).
        """
        try:
            # Make a copy to avoid modifying the original dict passed by the caller
            local_record_data = record_data.copy()

            # --- 1. Pre-process data (LTP parsing) ---
            ltp_val = local_record_data.get('ltp')
            if ltp_val and isinstance(ltp_val, str) and ' ' in ltp_val:
                parts = ltp_val.split(' ', 1)
                local_record_data['ltp'] = parts[0]
                local_record_data['price_percentage'] = parts[1]

            symbol = local_record_data.get('symbol')
            if not symbol:
                raise ValueError("Symbol is required for upsert")

            # Manually flush to ensure the session's state is queried
            self.session.flush()
            existing_record = self.session.query(SgRelativeStrength).filter_by(symbol=symbol).first()

            if existing_record:
                # --- 2. Handle Update ---

                # Special append logic for 'relstrength'
                if 'relstrength' in local_record_data:
                    existing_values = existing_record.relstrength or ""
                    new_value = local_record_data.pop('relstrength') # Pop to prevent overwrite

                    existing_set = set(val.strip() for val in existing_values.split(',') if val.strip())
                    new_set = set(val.strip() for val in new_value.split(',') if val.strip())

                    if not new_set.issubset(existing_set):
                        combined_set = existing_set.union(new_set)
                        existing_record.relstrength = ",".join(sorted(list(combined_set)))

                # Overwrite all other fields from the (potentially modified) local data
                for key, value in local_record_data.items():
                    setattr(existing_record, key, value)

                # Manually update the timestamp to reflect the update time
                existing_record.created_at = now_ist().isoformat()
                logger.info(f"Prepared update for {symbol}.")
            else:
                # --- 3. Handle Insert ---
                new_record = SgRelativeStrength(**local_record_data)
                self.session.add(new_record)
                logger.info(f"Prepared insert for {symbol}.")

        except Exception as e:
            logger.error(f"Error in upsert for symbol {record_data.get('symbol')}: {e}", exc_info=True)
            raise

    def bulk_upsert(self, records: list[dict]):
        """
        Upserts a list of records within the current session.
        The caller is responsible for transaction management.
        """
        for record in records:
            self.upsert(record)

        count = len(records)
        logger.info(f"Prepared upsert for {count} records.")
        return count

    def get_all(self, limit: int = 100):
        """Retrieves all records, up to a given limit."""
        try:
            query = select(SgRelativeStrength).limit(limit)
            result = self.session.execute(query).scalars().all()
            return result
        except SQLAlchemyError as e:
            logger.error(f"Error retrieving all records: {e}", exc_info=True)
            return []

    def get_by_symbol(self, symbol: str):
        """Retrieves a single record by its symbol."""
        try:
            query = select(SgRelativeStrength).where(SgRelativeStrength.symbol == symbol)
            result = self.session.execute(query).scalar_one_or_none()
            return result
        except SQLAlchemyError as e:
            logger.error(f"Error retrieving record by symbol {symbol}: {e}", exc_info=True)
            return None

    def delete_by_symbol(self, symbol: str):
        """Deletes records for a given symbol."""
        try:
            stmt = delete(SgRelativeStrength).where(SgRelativeStrength.symbol == symbol)
            result = self.session.execute(stmt)
            self.session.commit()
            logger.info(f"Deleted {result.rowcount} record(s) for symbol {symbol}.")
            return result.rowcount
        except SQLAlchemyError as e:
            logger.error(f"Error deleting record by symbol {symbol}: {e}", exc_info=True)
            self.session.rollback()
            return 0

    def delete_all(self):
        """Deletes all records from the table."""
        try:
            stmt = delete(SgRelativeStrength)
            result = self.session.execute(stmt)
            self.session.commit()
            logger.info(f"Deleted all {result.rowcount} records from the table.")
            return result.rowcount
        except SQLAlchemyError as e:
            logger.error(f"Error deleting all records: {e}", exc_info=True)
            self.session.rollback()
            return 0


# ---------------- MAIN BLOCK FOR DEMONSTRATION ----------------
if __name__ == "__main__":
    logger.info("Starting script execution...")
    create_tables()
    db_session = next(get_db_session())

    try:
        repo = SgRelativeStrengthRepository(db_session)

        # Clean up previous test data for a fresh run
        logger.info("--- Cleaning up old data with delete_all ---")
        repo.delete_all()
        # Commit the deletion immediately
        db_session.commit()

        # Sample data for insertion, including new formats to test parsing.
        sample_records = [
            {
                "symbol": "RELIANCE", "ltp": "2950.00 -0.5 %", "relstrength_value": "75.5", "relstrength": "Outperforming",
            },
            {
                "symbol": "TCS", "ltp": "3850.00", "relstrength_value": "62.1", "relstrength": "Neutral",
            },
        ]

        logger.info("--- Preparing Bulk Upsert (Insert) ---")
        repo.bulk_upsert(sample_records)

        logger.info("--- Preparing Single Upsert (Insert) ---")
        repo.upsert({
            "symbol": "INFY", "ltp": "1600.00 1.2 %", "relstrength_value": "45.0", "relstrength": "Underperforming",
        })

        logger.info("--- Preparing Upsert (Update) on 'TCS' to test append logic ---")
        repo.upsert({
            "symbol": "TCS", "ltp": "3900.50 2.5 %", "relstrength": "Trending",
        })

        logger.info("--- Committing all prepared changes ---")
        db_session.commit()
        logger.info("--- Transaction committed ---")

        # --- Verification steps after commit ---
        logger.info("--- Verifying LTP parsing for 'RELIANCE' ---")
        reliance_data = repo.get_by_symbol("RELIANCE")
        if reliance_data:
            logger.info(f"  - Found: {reliance_data}")
            if reliance_data.ltp == "2950.00" and reliance_data.price_percentage == "-0.5 %":
                logger.info("  - Verification PASSED: LTP and Price Percentage are correct.")
            else:
                logger.error("  - Verification FAILED: LTP or Price Percentage is incorrect.")
        else:
            logger.warning("  - 'RELIANCE' not found.")

        logger.info("--- Verifying relstrength append for 'TCS' ---")
        tcs_data = repo.get_by_symbol("TCS")
        if tcs_data:
            logger.info(f"  - Found updated: {tcs_data}")
            # Note: The order is sorted, so "Neutral" comes before "Trending"
            if tcs_data.relstrength == "Neutral,Trending":
                logger.info("  - Verification PASSED: relstrength was correctly appended.")
            else:
                logger.error(f"  - Verification FAILED: relstrength is '{tcs_data.relstrength}', expected 'Neutral,Trending'.")
        else:
            logger.warning("  - 'TCS' not found post-upsert.")

        logger.info("--- Deleting record for 'RELIANCE' ---")
        repo.delete_by_symbol("RELIANCE")
        db_session.commit() # Commit the deletion

        logger.info("--- Retrieving all records to verify deletion ---")
        all_data_after_delete = repo.get_all()
        if all_data_after_delete:
            for item in all_data_after_delete:
                logger.info(f"  - {item}")
        else:
            logger.warning("  No data found.")

    except Exception as e:
        logger.error(f"An error occurred during the transaction: {e}", exc_info=True)
        logger.info("Rolling back transaction...")
        db_session.rollback()
    finally:
        logger.info("Closing database session.")
        db_session.close()

    logger.info("--- Script execution finished ---")
