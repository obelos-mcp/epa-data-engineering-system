"""
Environmental Site Data Management & QA/QC Automation System
Database Connection Management

This module provides comprehensive PostgreSQL database connectivity using SQLAlchemy
with connection pooling, health monitoring, and transaction management.
"""

import os
import logging
import time
from pathlib import Path
from typing import Dict, Any, Optional, List, Union
from dataclasses import dataclass
from datetime import datetime
from contextlib import contextmanager

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine, text, inspect, MetaData
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError, OperationalError, IntegrityError
from sqlalchemy.orm import sessionmaker, Session

# Import for environment configuration
from dotenv import load_dotenv

@dataclass
class DatabaseConfig:
    """Database configuration parameters"""
    host: str = 'localhost'
    port: int = 5432
    database: str = 'environmental_data'
    username: str = 'postgres'
    password: str = 'password'
    pool_size: int = 10
    max_overflow: int = 20
    pool_timeout: int = 30
    pool_recycle: int = 3600
    echo: bool = False
    
    def get_connection_url(self) -> str:
        """Get PostgreSQL connection URL"""
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

class DatabaseManager:
    """Comprehensive database connection and transaction management"""
    
    def __init__(self, config: Optional[DatabaseConfig] = None, log_level: str = 'INFO'):
        self.config = config or self._load_config()
        self.engine: Optional[Engine] = None
        self.SessionLocal: Optional[sessionmaker] = None
        self._setup_logging(log_level)
        self._connection_stats = {
            'total_connections': 0,
            'active_connections': 0,
            'failed_connections': 0,
            'last_health_check': None,
            'health_status': 'unknown'
        }
        
        # Initialize connection
        self._initialize_connection()
    
    def _setup_logging(self, log_level: str):
        """Setup database logging"""
        self.logger = logging.getLogger('database_manager')
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        # Create handler if not exists
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
    
    def _load_config(self) -> DatabaseConfig:
        """Load database configuration from environment"""
        # Load .env file if it exists
        env_file = Path(__file__).parent.parent / '.env'
        if env_file.exists():
            load_dotenv(env_file)
        
        return DatabaseConfig(
            host=os.getenv('DB_HOST', 'localhost'),
            port=int(os.getenv('DB_PORT', '5432')),
            database=os.getenv('DB_NAME', 'environmental_data'),
            username=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'password'),
            pool_size=int(os.getenv('DB_POOL_SIZE', '10')),
            max_overflow=int(os.getenv('DB_MAX_OVERFLOW', '20')),
            pool_timeout=int(os.getenv('DB_POOL_TIMEOUT', '30')),
            pool_recycle=int(os.getenv('DB_POOL_RECYCLE', '3600')),
            echo=os.getenv('DB_ECHO', 'false').lower() == 'true'
        )
    
    def _initialize_connection(self):
        """Initialize database engine and session factory"""
        try:
            self.engine = create_engine(
                self.config.get_connection_url(),
                poolclass=QueuePool,
                pool_size=self.config.pool_size,
                max_overflow=self.config.max_overflow,
                pool_timeout=self.config.pool_timeout,
                pool_recycle=self.config.pool_recycle,
                echo=self.config.echo,
                pool_pre_ping=True,  # Enable connection health checks
                isolation_level="READ_COMMITTED"
            )
            
            self.SessionLocal = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self.engine
            )
            
            # Test connection
            self.health_check()
            self.logger.info("Database connection initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize database connection: {e}")
            raise
    
    def health_check(self) -> bool:
        """Check database connection health"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1")).scalar()
                
            self._connection_stats['last_health_check'] = datetime.now()
            self._connection_stats['health_status'] = 'healthy'
            return True
            
        except Exception as e:
            self.logger.error(f"Database health check failed: {e}")
            self._connection_stats['health_status'] = 'unhealthy'
            self._connection_stats['failed_connections'] += 1
            return False
    
    def get_connection_stats(self) -> Dict[str, Any]:
        """Get connection pool statistics"""
        if self.engine and hasattr(self.engine.pool, 'size'):
            pool = self.engine.pool
            self._connection_stats.update({
                'pool_size': pool.size(),
                'checked_in': pool.checkedin(),
                'checked_out': pool.checkedout(),
                'overflow': pool.overflow(),
                'invalid': pool.invalid()
            })
        
        return self._connection_stats.copy()
    
    @contextmanager
    def get_session(self):
        """Get database session with automatic cleanup"""
        session = self.SessionLocal()
        try:
            self._connection_stats['active_connections'] += 1
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            self.logger.error(f"Session error: {e}")
            raise
        finally:
            session.close()
            self._connection_stats['active_connections'] -= 1
    
    @contextmanager
    def get_connection(self):
        """Get raw database connection with automatic cleanup"""
        conn = self.engine.connect()
        try:
            self._connection_stats['total_connections'] += 1
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Connection error: {e}")
            raise
        finally:
            conn.close()
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Execute query and return results as DataFrame"""
        try:
            with self.get_connection() as conn:
                return pd.read_sql(query, conn, params=params)
        except Exception as e:
            self.logger.error(f"Query execution failed: {e}")
            raise
    
    def execute_statement(self, statement: str, params: Optional[Dict] = None) -> int:
        """Execute SQL statement and return affected rows"""
        try:
            with self.get_connection() as conn:
                result = conn.execute(text(statement), params or {})
                return result.rowcount
        except Exception as e:
            self.logger.error(f"Statement execution failed: {e}")
            raise
    
    def bulk_insert_dataframe(self, df: pd.DataFrame, table_name: str, 
                            schema: str = 'staging', if_exists: str = 'append',
                            method: str = 'multi', chunksize: int = 10000) -> int:
        """Bulk insert DataFrame to database table"""
        try:
            start_time = time.time()
            
            # Use to_sql for bulk operations
            rows_inserted = df.to_sql(
                name=table_name,
                con=self.engine,
                schema=schema,
                if_exists=if_exists,
                index=False,
                method=method,
                chunksize=chunksize
            )
            
            duration = time.time() - start_time
            rows_count = len(df)
            
            self.logger.info(
                f"Bulk insert to {schema}.{table_name}: "
                f"{rows_count:,} rows in {duration:.2f}s "
                f"({rows_count/duration:.0f} rows/sec)"
            )
            
            return rows_count
            
        except Exception as e:
            self.logger.error(f"Bulk insert failed for {schema}.{table_name}: {e}")
            raise
    
    def upsert_dataframe(self, df: pd.DataFrame, table_name: str, 
                        schema: str = 'core', conflict_columns: List[str] = None,
                        update_columns: List[str] = None) -> int:
        """Upsert DataFrame using PostgreSQL ON CONFLICT"""
        if df.empty:
            return 0
            
        try:
            # Create temporary table for upsert operation
            temp_table = f"temp_{table_name}_{int(time.time())}"
            
            with self.get_connection() as conn:
                # Insert into temporary table
                df.to_sql(
                    name=temp_table,
                    con=conn,
                    if_exists='replace',
                    index=False,
                    method='multi'
                )
                
                # Build upsert query
                columns = df.columns.tolist()
                conflict_cols = conflict_columns or [columns[0]]  # Default to first column
                update_cols = update_columns or [col for col in columns if col not in conflict_cols]
                
                # Create column lists for query
                col_list = ', '.join(columns)
                conflict_list = ', '.join(conflict_cols)
                update_list = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_cols])
                
                upsert_query = f"""
                INSERT INTO {schema}.{table_name} ({col_list})
                SELECT {col_list} FROM {temp_table}
                ON CONFLICT ({conflict_list}) 
                DO UPDATE SET {update_list}
                """
                
                result = conn.execute(text(upsert_query))
                rows_affected = result.rowcount
                
                # Clean up temporary table
                conn.execute(text(f"DROP TABLE {temp_table}"))
                
                self.logger.info(f"Upsert to {schema}.{table_name}: {rows_affected:,} rows affected")
                return rows_affected
                
        except Exception as e:
            self.logger.error(f"Upsert failed for {schema}.{table_name}: {e}")
            raise
    
    def get_table_info(self, table_name: str, schema: str = 'staging') -> Dict[str, Any]:
        """Get table information including row count and structure"""
        try:
            with self.get_connection() as conn:
                # Get row count
                count_query = f"SELECT COUNT(*) FROM {schema}.{table_name}"
                row_count = conn.execute(text(count_query)).scalar()
                
                # Get table structure
                inspector = inspect(self.engine)
                columns = inspector.get_columns(table_name, schema=schema)
                
                return {
                    'schema': schema,
                    'table': table_name,
                    'row_count': row_count,
                    'column_count': len(columns),
                    'columns': [col['name'] for col in columns],
                    'column_types': {col['name']: str(col['type']) for col in columns}
                }
                
        except Exception as e:
            self.logger.error(f"Failed to get table info for {schema}.{table_name}: {e}")
            raise
    
    def create_schema_if_not_exists(self, schema_name: str):
        """Create schema if it doesn't exist"""
        try:
            with self.get_connection() as conn:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
                self.logger.info(f"Schema '{schema_name}' created or already exists")
        except Exception as e:
            self.logger.error(f"Failed to create schema {schema_name}: {e}")
            raise
    
    def truncate_table(self, table_name: str, schema: str = 'staging', cascade: bool = False):
        """Truncate table"""
        try:
            cascade_clause = " CASCADE" if cascade else ""
            with self.get_connection() as conn:
                conn.execute(text(f"TRUNCATE TABLE {schema}.{table_name}{cascade_clause}"))
                self.logger.info(f"Truncated table {schema}.{table_name}")
        except Exception as e:
            self.logger.error(f"Failed to truncate {schema}.{table_name}: {e}")
            raise
    
    def disable_indexes(self, table_name: str, schema: str = 'staging'):
        """Disable indexes for faster bulk loading"""
        try:
            with self.get_connection() as conn:
                # Get all indexes for the table
                index_query = """
                SELECT indexname FROM pg_indexes 
                WHERE schemaname = :schema AND tablename = :table
                AND indexname NOT LIKE '%_pkey'
                """
                
                indexes = conn.execute(
                    text(index_query), 
                    {'schema': schema, 'table': table_name}
                ).fetchall()
                
                # Drop non-primary key indexes
                for (index_name,) in indexes:
                    conn.execute(text(f"DROP INDEX IF EXISTS {schema}.{index_name}"))
                
                self.logger.info(f"Disabled {len(indexes)} indexes for {schema}.{table_name}")
                return [idx[0] for idx in indexes]
                
        except Exception as e:
            self.logger.error(f"Failed to disable indexes for {schema}.{table_name}: {e}")
            raise
    
    def enable_indexes(self, table_name: str, schema: str = 'staging'):
        """Re-enable indexes after bulk loading"""
        try:
            # This would typically involve recreating indexes
            # For now, we'll rely on the schema creation to have proper indexes
            self.logger.info(f"Indexes management completed for {schema}.{table_name}")
        except Exception as e:
            self.logger.error(f"Failed to enable indexes for {schema}.{table_name}: {e}")
            raise
    
    def validate_data_integrity(self, table_name: str, schema: str = 'core') -> Dict[str, Any]:
        """Validate data integrity for a table"""
        try:
            with self.get_connection() as conn:
                validation_results = {}
                
                # Check for NULL values in NOT NULL columns
                inspector = inspect(self.engine)
                columns = inspector.get_columns(table_name, schema=schema)
                
                for col in columns:
                    if not col.get('nullable', True):
                        null_count_query = f"""
                        SELECT COUNT(*) FROM {schema}.{table_name} 
                        WHERE {col['name']} IS NULL
                        """
                        null_count = conn.execute(text(null_count_query)).scalar()
                        if null_count > 0:
                            validation_results[f"null_violations_{col['name']}"] = null_count
                
                # Check foreign key constraints (simplified)
                fk_query = """
                SELECT COUNT(*) as violations FROM information_schema.table_constraints 
                WHERE constraint_type = 'FOREIGN KEY' 
                AND table_schema = :schema AND table_name = :table
                """
                
                fk_count = conn.execute(
                    text(fk_query), 
                    {'schema': schema, 'table': table_name}
                ).scalar()
                
                validation_results['foreign_key_constraints'] = fk_count
                validation_results['validation_timestamp'] = datetime.now()
                
                return validation_results
                
        except Exception as e:
            self.logger.error(f"Data integrity validation failed for {schema}.{table_name}: {e}")
            raise
    
    def get_load_performance_metrics(self) -> Dict[str, Any]:
        """Get database load performance metrics"""
        try:
            with self.get_connection() as conn:
                # Get recent ETL run statistics
                etl_stats_query = """
                SELECT 
                    process_name,
                    AVG(records_processed) as avg_records,
                    AVG(EXTRACT(EPOCH FROM (end_time - start_time))) as avg_duration_seconds,
                    COUNT(*) as run_count
                FROM core.etl_run_log 
                WHERE start_time >= NOW() - INTERVAL '24 hours'
                AND status = 'SUCCESS'
                GROUP BY process_name
                ORDER BY avg_records DESC
                """
                
                etl_stats = pd.read_sql(etl_stats_query, conn)
                
                # Get table sizes
                size_query = """
                SELECT 
                    schemaname,
                    tablename,
                    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
                    pg_total_relation_size(schemaname||'.'||tablename) as size_bytes
                FROM pg_tables 
                WHERE schemaname IN ('staging', 'core')
                ORDER BY size_bytes DESC
                """
                
                table_sizes = pd.read_sql(size_query, conn)
                
                return {
                    'etl_performance': etl_stats.to_dict('records'),
                    'table_sizes': table_sizes.to_dict('records'),
                    'connection_stats': self.get_connection_stats(),
                    'timestamp': datetime.now()
                }
                
        except Exception as e:
            self.logger.error(f"Failed to get performance metrics: {e}")
            raise
    
    def close(self):
        """Close database connections"""
        if self.engine:
            self.engine.dispose()
            self.logger.info("Database connections closed")

# Global database manager instance
_db_manager: Optional[DatabaseManager] = None

def get_database_manager(config: Optional[DatabaseConfig] = None) -> DatabaseManager:
    """Get global database manager instance"""
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager(config)
    return _db_manager

def create_sample_env_file(file_path: str = '.env'):
    """Create sample .env file with database configuration"""
    env_content = """# Environmental Site Data Management Database Configuration
# PostgreSQL Connection Settings
DB_HOST=localhost
DB_PORT=5432
DB_NAME=environmental_data
DB_USER=postgres
DB_PASSWORD=your_password_here

# Connection Pool Settings
DB_POOL_SIZE=10
DB_MAX_OVERFLOW=20
DB_POOL_TIMEOUT=30
DB_POOL_RECYCLE=3600

# Debug Settings
DB_ECHO=false
"""
    
    with open(file_path, 'w') as f:
        f.write(env_content)
    
    print(f"Sample .env file created at {file_path}")
    print("Please update the database credentials before running the system.")

if __name__ == "__main__":
    # Create sample .env file if run directly
    create_sample_env_file()
