from db import DatabaseConnection
from typing import List, Dict, Any
from datetime import datetime

def map_user_data(old_user: Dict[str, Any]) -> Dict[str, Any]:
    """
    이전 user 테이블 데이터를 새로운 user 테이블 구조에 맞게 변환합니다.
    
    예시: 이전 테이블과 새 테이블의 구조 차이
    
    이전 테이블 (attendance-dev.user):
    - id: int
    - username: varchar
    - email: varchar 
    - first_name: varchar
    - last_name: varchar
    - created_at: datetime
    - is_active: tinyint
    
    새 테이블 (attendance_renew_dev.user):
    - user_id: int (이전 id)
    - full_name: varchar (first_name + last_name)
    - email_address: varchar (이전 email)
    - login_name: varchar (이전 username)
    - status: enum('active', 'inactive') (이전 is_active)
    - created_date: datetime (이전 created_at)
    - updated_date: datetime (새로 추가)
    """
    
    # 새로운 테이블 구조에 맞게 데이터 매핑
    mapped_data = {
        'id': old_user['id'],
        'name': old_user['name'],
        'name_suffix': old_user['name_suffix'],
        'email': old_user['email'],
        'password': old_user['password'],
        'gender': old_user['gender_type'],
        'birth_date': old_user['birth_date'],
        'phone_number': old_user['phone_number'],
        'is_new_member': old_user['is_new_member'],
        'is_long_term_absentee': old_user['is_long_term_absentee'],
        'is_deleted': old_user['is_deleted'],
        'created_at': old_user['created_at'],
        'updated_at': old_user['updated_at']
    }
    
    # None 값이나 빈 문자열 처리
    if not mapped_data['full_name']:
        mapped_data['full_name'] = mapped_data['login_name']  # fallback
    
    return mapped_data


def migrate_user_table(before: DatabaseConnection, after: DatabaseConnection):
    """user 테이블 데이터 마이그레이션을 수행합니다."""
    
    print("👤 User 테이블 마이그레이션을 시작합니다...")
    
    try:
        # 1. 이전 데이터 조회
        print("📖 이전 user 데이터를 조회합니다...")
        before_user_data = before.execute_query("SELECT * FROM user ORDER BY id")
        
        if not before_user_data:
            print("⚠️ 이전 user 테이블에 데이터가 없습니다.")
            return True
        
        print(f"📊 총 {len(before_user_data)}개의 user 레코드를 찾았습니다.")
        
        # 2. 데이터 변환 (map 사용)
        print("🔄 데이터를 새로운 구조에 맞게 변환합니다...")
        mapped_users = list(map(map_user_data, before_user_data))
        
        # 3. 새로운 테이블에 삽입할 쿼리 준비
        insert_query = """
        INSERT INTO user (
            user_id, full_name, email_address, login_name, 
            status, created_date, updated_date
        ) VALUES (
            %(user_id)s, %(full_name)s, %(email_address)s, %(login_name)s,
            %(status)s, %(created_date)s, %(updated_date)s
        )
        """
        
        # 4. 배치 삽입 실행
        print("💾 새로운 user 테이블에 데이터를 삽입합니다...")
        rows_affected = after.execute_many(insert_query, mapped_users)
        
        if rows_affected:
            print(f"✅ {rows_affected}개의 user 레코드가 성공적으로 마이그레이션되었습니다.")   
            return True
        else:
            print("영향받은 레코드가 없습니다.")
            return False
            
    except Exception as e:
        print(f"❌ User 테이블 마이그레이션 중 오류 발생: {e}")
        return False
