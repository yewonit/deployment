from db import DatabaseConnection
from utils.progress import chunked, print_progress
from typing import List, Dict, Any
from datetime import datetime

def map_attendance_data(old_attendance: Dict[str, Any]) -> Dict[str, Any]:
    """
    이전 image 테이블 데이터를 새로운 image 테이블 구조에 맞게 변환합니다.
    """
    
    mapped_data = {
      'id': old_attendance['id'],
      'user_id': old_attendance['user_id'],
      'activity_id': old_attendance['activity_id'],
      'attendance_status': old_attendance['attendance_status'],
      'created_at': old_attendance['created_at'],
      'updated_at': old_attendance['updated_at']
    }
    
    return mapped_data
  
def migrate_attendance_table(before: DatabaseConnection, after: DatabaseConnection):
    """
    이전 attendance 테이블 데이터를 새로운 attendance 테이블로 마이그레이션합니다.
    """
    try:
        before_attendance_data = before.execute_query(
        """
        SELECT
          attendance.id as id,
          attendance.user_id as user_id,
          attendance.activity_instance_id as activity_id,
          attendance_status.name as attendance_status,
          attendance.created_at as created_at,
          attendance.updated_at as updated_at
        FROM attendance
        INNER JOIN attendance_status
        ON attendance.attendance_status_id = attendance_status.id
        """
        )
        
        if not before_attendance_data:
            print("⚠️ 이전 attendance 테이블에 데이터가 없습니다.")
            return True
        
        mapped_attendances = list(map(map_attendance_data, before_attendance_data))
        
        insert_query = """
        INSERT INTO attendance (
          id, user_id, activity_id, attendance_status, created_at, updated_at
        ) VALUES (
          %(id)s, %(user_id)s, %(activity_id)s, %(attendance_status)s, %(created_at)s, %(updated_at)s
        ) ON DUPLICATE KEY UPDATE id=id
        """
        
        total = len(mapped_attendances)
        affected_total = 0
        batch_size = 1000
        processed = 0
        for batch in chunked(mapped_attendances, batch_size):
            affected = after.execute_many(insert_query, batch) or 0
            affected_total += affected
            processed += len(batch)
            print_progress(processed, total, prefix="attendance")
        
        if affected_total:
            print(f"✅ {affected_total}개의 attendance 레코드가 성공적으로 마이그레이션되었습니다.")
            return True
        else:
            print("영향받은 레코드가 없습니다.")
            return False
        
    except Exception as e:
        print(f"❌ attendance 테이블 마이그레이션 중 오류 발생: {e}")
        return False


def migrate_attendance_prod_to_dev(prod: DatabaseConnection, dev: DatabaseConnection):
    """
    prod.attendance의 모든 데이터를 dev.attendance로 마이그레이션.
    - prod/dev 스키마 동일, 필드 1:1 복사
    - dev.user에 없는 user_id는 스킵
    - dev.activity에 없는 activity_id는 스킵 (activity 스킵 시 해당 attendance도 모두 스킵)
    """
    try:
        dev_user_rows = dev.execute_query("SELECT id FROM user") or []
        dev_user_ids = {row['id'] for row in dev_user_rows}
        
        dev_activity_rows = dev.execute_query("SELECT id FROM activity") or []
        dev_activity_ids = {row['id'] for row in dev_activity_rows}
        
        prod_attendances = prod.execute_query(
            """
            SELECT id, user_id, activity_id, attendance_status, created_at, updated_at
            FROM attendance
            ORDER BY id
            """
        ) or []
        
        to_insert: List[Dict[str, Any]] = []
        skipped_no_user = 0
        skipped_no_activity = 0
        for r in prod_attendances:
            if r['user_id'] not in dev_user_ids:
                skipped_no_user += 1
                continue
            if r['activity_id'] not in dev_activity_ids:
                skipped_no_activity += 1
                continue
            to_insert.append(r)
        
        if not to_insert:
            print("ℹ️ 마이그레이션할 attendance 데이터가 없습니다.")
            return True
        
        insert_sql = """
        INSERT INTO attendance (
            id, user_id, activity_id, attendance_status, created_at, updated_at
        ) VALUES (
            %(id)s, %(user_id)s, %(activity_id)s, %(attendance_status)s, %(created_at)s, %(updated_at)s
        ) ON DUPLICATE KEY UPDATE id=id
        """
        total = len(to_insert)
        affected_total = 0
        batch_size = 1000
        processed = 0
        for batch in chunked(to_insert, batch_size):
            affected = dev.execute_many(insert_sql, batch) or 0
            affected_total += affected
            processed += len(batch)
            print_progress(processed, total, prefix="attendance(prod->dev)")
        print(f"✅ attendance 마이그레이션 완료: 삽입/갱신 {affected_total}건, 사용자 미존재 스킵 {skipped_no_user}건, 활동 미존재 스킵 {skipped_no_activity}건")
        return True
    except Exception as e:
        print(f"❌ attendance prod->dev 마이그레이션 오류: {e}")
        return False
        