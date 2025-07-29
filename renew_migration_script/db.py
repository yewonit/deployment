import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv
from typing import Optional

# .env 파일 로드
load_dotenv()

class DatabaseConnection:
    """MySQL 데이터베이스 연결을 관리하는 클래스"""
    
    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection: Optional[mysql.connector.MySQLConnection] = None
        self.cursor = None
    
    def connect(self):
        """데이터베이스에 연결합니다."""
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                charset='utf8mb4',
                collation='utf8mb4_unicode_ci',
                autocommit=False
            )
            self.cursor = self.connection.cursor(dictionary=True)
            print(f"✅ {self.database} 스키마에 성공적으로 연결되었습니다.")
            return True
        except Error as e:
            print(f"❌ 데이터베이스 연결 실패: {e}")
            return False
    
    def disconnect(self):
        """데이터베이스 연결을 종료합니다."""
        if self.cursor:
            self.cursor.close()
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print(f"🔌 {self.database} 스키마 연결이 종료되었습니다.")
    
    def execute_query(self, query: str, params=None):
        """쿼리를 실행합니다."""
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            return self.cursor.fetchall()
        except Error as e:
            print(f"❌ 쿼리 실행 실패: {e}")
            print(f"쿼리: {query}")
            return None
    
    def execute_insert(self, query: str, params=None):
        """INSERT 쿼리를 실행합니다."""
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            self.connection.commit()
            return self.cursor.lastrowid
        except Error as e:
            print(f"❌ INSERT 쿼리 실행 실패: {e}")
            print(f"쿼리: {query}")
            self.connection.rollback()
            return None
    
    def execute_many(self, query: str, data_list):
        """여러 개의 데이터를 한번에 INSERT합니다."""
        try:
            self.cursor.executemany(query, data_list)
            self.connection.commit()
            return self.cursor.rowcount
        except Error as e:
            print(f"❌ Batch INSERT 실행 실패: {e}")
            print(f"쿼리: {query}")
            self.connection.rollback()
            return None
    
    def get_table_list(self):
        """데이터베이스의 테이블 목록을 가져옵니다."""
        query = "SHOW TABLES"
        result = self.execute_query(query)
        if result:
            table_key = f"Tables_in_{self.database}"
            return [row[table_key] for row in result]
        return []
    
    def get_table_schema(self, table_name: str):
        """테이블의 스키마 정보를 가져옵니다."""
        query = f"DESCRIBE {table_name}"
        return self.execute_query(query)


def create_database_connection(schema_name: str) -> DatabaseConnection:
    """환경 변수를 사용하여 데이터베이스 연결 객체를 생성합니다."""
    host = os.getenv('DB_HOST')
    port = int(os.getenv('DB_PORT', 3306))
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    
    if not all([host, user, password]):
        raise ValueError("데이터베이스 연결 정보가 완전하지 않습니다. .env 파일을 확인해주세요.")
    
    return DatabaseConnection(host, port, user, password, schema_name)


def test_connection(db_connection: DatabaseConnection) -> bool:
    """데이터베이스 연결을 테스트합니다."""
    if db_connection.connect():
        # 간단한 쿼리로 연결 테스트
        result = db_connection.execute_query("SELECT 1")
        db_connection.disconnect()
        return result is not None
    return False
