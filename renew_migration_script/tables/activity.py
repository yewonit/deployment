from db import DatabaseConnection
from typing import List, Dict, Any
from datetime import datetime

def map_activity_data(old_activity: Dict[str, Any]) -> Dict[str, Any]:
    """
    이전 image 테이블 데이터를 새로운 image 테이블 구조에 맞게 변환합니다.
    """
    
    mapped_data = {
      'id': old_activity['id'],
      'name': old_activity['name'],
      'description': old_activity['description'],
      'activity_category': '예배',
      'location': old_activity['location'],
      'organization_id': old_activity['organization_id'],
      'start_time': old_activity['start_time'],
      'end_time': old_activity['end_time'],
      'is_deleted': 0,
      'created_at': old_activity['created_at'],
      'updated_at': old_activity['updated_at']
    }
    
    return mapped_data
  
def migrate_activity_table(before: DatabaseConnection, after: DatabaseConnection):
    """
    이전 activity 테이블 데이터를 새로운 activity 테이블로 마이그레이션합니다.
    """
    try:
        before_activity_data = before.execute_query(
        """
        SELECT
          activity_instance.id as id,
          activity.name as name,
          activity_instance.notes as description,
          activity_instance.actual_location as location,
          activity.organization_id as organization_id,
          activity_instance.start_datetime as start_time,
          activity_instance.end_datetime as end_time,
          activity_instance.created_at as created_at,
          activity_instance.updated_at as updated_at
        FROM activity_instance
        INNER JOIN activity
        ON activity_instance.activity_id = activity.id
        """
        )
        
        if not before_activity_data:
            print("⚠️ 이전 activity 테이블에 데이터가 없습니다.")
            return True
        
        mapped_activities = list(map(map_activity_data, before_activity_data))
        
        insert_query = """
        INSERT INTO activity (
          id, name, description, activity_category, location, organization_id, start_time, end_time, is_deleted, created_at, updated_at
        ) VALUES (
          %(id)s, %(name)s, %(description)s, %(activity_category)s, %(location)s, %(organization_id)s, %(start_time)s, %(end_time)s, %(is_deleted)s, %(created_at)s, %(updated_at)s
        ) ON DUPLICATE KEY UPDATE id=id
        """
        
        rows_affected = after.execute_many(insert_query, mapped_activities)
        
        if rows_affected:
            print(f"✅ {rows_affected}개의 activity 레코드가 성공적으로 마이그레이션되었습니다.")
            return True
        else:
            print("영향받은 레코드가 없습니다.")
            return False
        
    except Exception as e:
        print(f"❌ activity 테이블 마이그레이션 중 오류 발생: {e}")
        return False


def migrate_activity_prod_to_dev(prod: DatabaseConnection, dev: DatabaseConnection):
    """
    최근 1개월 prod.activity 데이터를 dev.activity로 마이그레이션.
    - prod/dev 스키마 동일, 필드 1:1 복사
    - dev.organization에 없는 organization_id는 스킵
    """
    try:
        # dev 조직 존재 목록 수집
        dev_org_rows = dev.execute_query("SELECT id FROM organization") or []
        dev_org_ids = {row['id'] for row in dev_org_rows}
        
        # 최근 1개월 데이터 조회 (created_at 기준)
        prod_activities = prod.execute_query(
            """
            SELECT id, name, description, activity_category, organization_id, start_time, end_time, is_deleted, created_at, updated_at
            FROM activity
            WHERE created_at >= DATE_SUB(NOW(), INTERVAL 1 MONTH)
            ORDER BY id
            """
        ) or []
        
        to_insert: List[Dict[str, Any]] = []
        skipped_no_org = 0
        for a in prod_activities:
            if a['organization_id'] not in dev_org_ids:
                skipped_no_org += 1
                continue
            to_insert.append(a)
        
        if not to_insert:
            print("ℹ️ 마이그레이션할 activity 데이터가 없습니다.")
            return True
        
        insert_sql = """
        INSERT INTO activity (
            id, name, description, activity_category, organization_id, start_time, end_time, is_deleted, created_at, updated_at
        ) VALUES (
            %(id)s, %(name)s, %(description)s, %(activity_category)s, %(organization_id)s, %(start_time)s, %(end_time)s, %(is_deleted)s, %(created_at)s, %(updated_at)s
        ) ON DUPLICATE KEY UPDATE id=id
        """
        affected = dev.execute_many(insert_sql, to_insert)
        print(f"✅ activity 마이그레이션 완료: 삽입/갱신 {affected or 0}건, 조직 미존재로 스킵 {skipped_no_org}건")
        return True
    except Exception as e:
        print(f"❌ activity prod->dev 마이그레이션 오류: {e}")
        return False
