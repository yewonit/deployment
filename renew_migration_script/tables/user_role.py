from db import DatabaseConnection
from utils.progress import chunked, print_progress
from typing import List, Dict, Any
from datetime import datetime

def map_user_role_data(old_user_role: Dict[str, Any]) -> Dict[str, Any]:
    """
    이전 image 테이블 데이터를 새로운 image 테이블 구조에 맞게 변환합니다.
    """
    if old_user_role['role'] == '그룹장':
      role_id = 1
    elif old_user_role['role'] == '부그룹장':
      role_id = 2
    elif old_user_role['role'] == '순장':
      role_id = 3
    elif old_user_role['role'] == 'EBS':
      role_id = 4
    else:
      role_id = 5

    mapped_data = {
      'id': old_user_role['id'],
      'user_id': old_user_role['user_id'],
      'role_id': role_id,
      'organization_id': old_user_role['organization_id'],
      'created_at': old_user_role['created_at'],
      'updated_at': old_user_role['updated_at']
    }
    
    return mapped_data

def migrate_user_role_table(before: DatabaseConnection, after: DatabaseConnection):
    """
    이전 user_role 테이블 데이터를 새로운 user_role 테이블로 마이그레이션합니다.
    """
    try:
        before_user_role_data = before.execute_query(
        """
        SELECT 
          user_has_role.id as id,
          user_has_role.user_id as user_id,
          role.role_name as role,
          user_has_role.organization_id as organization_id,
          user_has_role.created_at as created_at,
          user_has_role.updated_at as updated_at
        FROM user_has_role
        INNER JOIN role ON user_has_role.role_id = role.id
        """
        )
        
        if not before_user_role_data:
            print("⚠️ 이전 user_role 테이블에 데이터가 없습니다.")
            return True
        
        mapped_user_roles = list(map(map_user_role_data, before_user_role_data))
        
        insert_query = """
        INSERT INTO user_role (
          id, user_id, role_id, organization_id, created_at, updated_at
        ) VALUES (
          %(id)s, %(user_id)s, %(role_id)s, %(organization_id)s, %(created_at)s, %(updated_at)s
        ) ON DUPLICATE KEY UPDATE id=id
        """
        
        total = len(mapped_user_roles)
        affected_total = 0
        batch_size = 1000
        processed = 0
        for batch in chunked(mapped_user_roles, batch_size):
            affected = after.execute_many(insert_query, batch) or 0
            affected_total += affected
            processed += len(batch)
            print_progress(processed, total, prefix="user_role")
        
        if affected_total:
            print(f"✅ {affected_total}개의 user_role 레코드가 성공적으로 마이그레이션되었습니다.")
            return True
        else:
            print("영향받은 레코드가 없습니다.")
            return False
        
    except Exception as e:
        print(f"❌ user_role 테이블 마이그레이션 중 오류 발생: {e}")
        return False
