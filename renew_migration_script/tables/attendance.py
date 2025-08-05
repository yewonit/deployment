from db import DatabaseConnection
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
        
        rows_affected = after.execute_many(insert_query, mapped_attendances)
        
        if rows_affected:
            print(f"✅ {rows_affected}개의 attendance 레코드가 성공적으로 마이그레이션되었습니다.")
            return True
        else:
            print("영향받은 레코드가 없습니다.")
            return False
        
    except Exception as e:
        print(f"❌ attendance 테이블 마이그레이션 중 오류 발생: {e}")
        return False
        