from db import DatabaseConnection
from typing import List, Dict, Any
from datetime import datetime

def map_organization_data(old_organization: Dict[str, Any]) -> Dict[str, Any]:
    """
    이전 image 테이블 데이터를 새로운 image 테이블 구조에 맞게 변환합니다.
    """
    
    mapped_data = {
      'id': old_organization['id'],
      'season_id': old_organization['season_id'],
      'organization_code': old_organization['organization_code'],
      'upper_organization_id': old_organization['upper_organization_id'],
      'is_deleted': old_organization['is_deleted'],
      'created_at': old_organization['created_at'],
      'updated_at': old_organization['updated_at']
    }
    
    return mapped_data
  
def migrate_organization_table(before: DatabaseConnection, after: DatabaseConnection):
    """
    이전 organization 테이블 데이터를 새로운 organization 테이블로 마이그레이션합니다.
    """
    try:
        before_organization_data = before.execute_query(
        """
        SELECT * 
        FROM organization
        """
        )
        
        if not before_organization_data:
            print("⚠️ 이전 organization 테이블에 데이터가 없습니다.")
            return True
        
        mapped_organizations = list(map(map_organization_data, before_organization_data))
        
        insert_query = """
        INSERT INTO organization (
          id, season_id, organization_code, upper_organization_id, is_deleted, created_at, updated_at
        ) VALUES (
          %(id)s, %(season_id)s, %(organization_code)s, %(upper_organization_id)s, %(is_deleted)s, %(created_at)s, %(updated_at)s
        )
        """
        
        rows_affected = after.execute_many(insert_query, mapped_organizations)
        
        if rows_affected:
            print(f"✅ {rows_affected}개의 organization 레코드가 성공적으로 마이그레이션되었습니다.")
            return True
        else:
            print("영향받은 레코드가 없습니다.")
            return False
        
    except Exception as e:
        print(f"❌ organization 테이블 마이그레이션 중 오류 발생: {e}")
        return False
  