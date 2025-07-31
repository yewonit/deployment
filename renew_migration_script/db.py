import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv
from typing import Optional
from sshtunnel import SSHTunnelForwarder

# .env 파일 로드
load_dotenv()

class DatabaseConnection:
    """SSH 터널을 통해 MySQL 데이터베이스 연결을 관리하는 클래스"""
    
    def __init__(self, ssh_host: str, ssh_port: int, ssh_username: str, ssh_pkey_path: str,
                 db_host: str, db_port: int, db_user: str, db_password: str, database: str):
        self.ssh_host = ssh_host
        self.ssh_port = ssh_port
        self.ssh_username = ssh_username
        self.ssh_pkey_path = ssh_pkey_path
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_password = db_password
        self.database = database
        self.connection: Optional[mysql.connector.MySQLConnection] = None
        self.cursor = None
        self.tunnel: Optional[SSHTunnelForwarder] = None
    
    def connect_direct(self):
        """SSH 터널 없이 직접 데이터베이스에 연결합니다."""
        try:
            # MySQL 직접 연결
            self.connection = mysql.connector.connect(
                host=self.db_host,
                port=self.db_port,
                user=self.db_user,
                password=self.db_password,
                database=self.database,
                charset='utf8mb4',
                collation='utf8mb4_unicode_ci',
                autocommit=False,
                use_unicode=True,
                connection_timeout=60
            )
            self.cursor = self.connection.cursor(dictionary=True)
            print(f"✅ {self.database} 스키마에 직접 연결되었습니다.")
            return True
        except Exception as e:
            print(f"❌ 데이터베이스 직접 연결 실패: {e}")
            return False
    
    def connect(self):
        """SSH 터널을 통해 데이터베이스에 연결합니다."""
        try:
            # SSH 터널 생성 (DataGrip과 동일한 설정)
            self.tunnel = SSHTunnelForwarder(
                (self.ssh_host, self.ssh_port),
                ssh_username=self.ssh_username,
                ssh_pkey=self.ssh_pkey_path,
                remote_bind_address=(self.db_host, self.db_port),
                local_bind_address=('127.0.0.1', 0),  # 로컬 포트는 자동 할당
                allow_agent=False,
                set_keepalive=60.0
            )
            
            # SSH 터널 시작
            self.tunnel.start()
            print(f"🔗 SSH 터널이 시작되었습니다: {self.ssh_host}:{self.ssh_port}")
            print(f"📍 로컬 포트: {self.tunnel.local_bind_port}")
            
            # MySQL 연결 (localhost를 통해)
            self.connection = mysql.connector.connect(
                host='localhost',  # 127.0.0.1 대신 localhost 사용
                port=self.tunnel.local_bind_port,
                user=self.db_user,
                password=self.db_password,
                database=self.database,
                charset='utf8mb4',
                collation='utf8mb4_unicode_ci',
                autocommit=False,
                use_unicode=True,
                connection_timeout=60
            )
            self.cursor = self.connection.cursor(dictionary=True)
            print(f"✅ {self.database} 스키마에 SSH 터널을 통해 성공적으로 연결되었습니다.")
            return True
        except Exception as e:
            print(f"❌ SSH 터널 또는 데이터베이스 연결 실패: {e}")
            if self.tunnel and self.tunnel.is_active:
                self.tunnel.stop()
            return False
    
    def disconnect(self):
        """데이터베이스 연결과 SSH 터널을 종료합니다."""
        if self.cursor:
            self.cursor.close()
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print(f"🔌 {self.database} 스키마 연결이 종료되었습니다.")
        
        if self.tunnel and self.tunnel.is_active:
            self.tunnel.stop()
            print(f"🔌 SSH 터널이 종료되었습니다.")
    
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


def create_database_connection(schema_name: str, use_ssh_tunnel: bool = True) -> DatabaseConnection:
    """환경 변수를 사용하여 데이터베이스 연결 객체를 생성합니다."""
    if use_ssh_tunnel:
        # SSH 터널을 통한 연결
        ssh_host = os.getenv('SSH_HOST', '15.164.45.130')
        ssh_port = int(os.getenv('SSH_PORT', 22))
        ssh_username = os.getenv('SSH_USERNAME', 'ec2-user')
        
        # SSH 키 파일 경로를 절대 경로로 설정
        ssh_pkey_path = os.getenv('SSH_PKEY_PATH')
        if not ssh_pkey_path:
            # 기본 경로 설정
            current_dir = os.path.dirname(os.path.abspath(__file__))
            ssh_pkey_path = os.path.join(os.path.dirname(current_dir), 'isaac-keypair.pem.secret')
        
        # 데이터베이스 설정 (SSH 터널을 통해 접근)
        db_host = os.getenv('DB_HOST', 'localhost')  # SSH 터널을 통해 접근하므로 localhost
        db_port = int(os.getenv('DB_PORT', 3306))
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASSWORD')
        
        if not all([db_user, db_password]):
            raise ValueError("데이터베이스 연결 정보가 완전하지 않습니다. .env 파일을 확인해주세요.")
        
        print(f"🔧 SSH 설정: {ssh_host}:{ssh_port}, 사용자: {ssh_username}")
        print(f"🔧 SSH 키 파일: {ssh_pkey_path}")
        print(f"🔧 DB 설정: {db_host}:{db_port}, 사용자: {db_user}")
        
        return DatabaseConnection(
            ssh_host=ssh_host,
            ssh_port=ssh_port,
            ssh_username=ssh_username,
            ssh_pkey_path=ssh_pkey_path,
            db_host=db_host,
            db_port=db_port,
            db_user=db_user,
            db_password=db_password,
            database=schema_name
        )
    else:
        # 직접 연결 (SSH 터널 없이)
        host = os.getenv('DB_HOST')
        port = int(os.getenv('DB_PORT', 3306))
        user = os.getenv('DB_USER')
        password = os.getenv('DB_PASSWORD')
        
        if not all([host, user, password]):
            raise ValueError("데이터베이스 연결 정보가 완전하지 않습니다. .env 파일을 확인해주세요.")
        
        # SSH 터널 없이 직접 연결하는 경우를 위한 임시 설정
        return DatabaseConnection(
            ssh_host='localhost',
            ssh_port=22,
            ssh_username='dummy',
            ssh_pkey_path='dummy',
            db_host=host,
            db_port=port,
            db_user=user,
            db_password=password,
            database=schema_name
        )


def test_connection(db_connection: DatabaseConnection) -> bool:
    """데이터베이스 연결을 테스트합니다."""
    if db_connection.connect():
        # 간단한 쿼리로 연결 테스트
        result = db_connection.execute_query("SELECT 1")
        db_connection.disconnect()
        return result is not None
    return False
