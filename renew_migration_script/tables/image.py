from db import DatabaseConnection
from utils.progress import chunked, print_progress
from typing import List, Dict, Any
from datetime import datetime

def map_image_data(old_image: Dict[str, Any]) -> Dict[str, Any]:
    """
    이전 image 테이블 데이터를 새로운 image 테이블 구조에 맞게 변환합니다.
    """
    
    mapped_data = {
      'id': old_image['id'],
      'activity_id': old_image['activity_id'],
      'name': old_image['name'],
      'path': old_image['path'],
      'is_deleted': 0,
      'created_at': old_image['created_at'],
      'updated_at': old_image['updated_at']
    }
    
    return mapped_data
    
def migrate_image_table(before: DatabaseConnection, after: DatabaseConnection):
    """
    이전 image 테이블 데이터를 새로운 image 테이블로 마이그레이션합니다.
    """
    try:
        before_image_data = before.execute_query(
        """
        SELECT 
          file.id as id,
          activity_instance_has_file.activity_instance_id as activity_id,
          file.file_name as name,
          file.file_path as path,
          file.created_at as created_at,
          file.updated_at as updated_at
        FROM file 
        inner join activity_instance_has_file 
        on file.id = activity_instance_has_file.file_id 
        ORDER BY file.id
        """
        )
      
        if not before_image_data:
            print("⚠️ 이전 image 테이블에 데이터가 없습니다.")
            return True
        
        mapped_images = list(map(map_image_data, before_image_data))
      
        insert_query = """
        INSERT INTO activity_image (
          id, activity_id, name, path, is_deleted, created_at, updated_at
        ) VALUES (
          %(id)s, %(activity_id)s, %(name)s, %(path)s, %(is_deleted)s, %(created_at)s, %(updated_at)s
        ) ON DUPLICATE KEY UPDATE id=id
        """
      
        total = len(mapped_images)
        affected_total = 0
        batch_size = 1000
        processed = 0
        for batch in chunked(mapped_images, batch_size):
            affected = after.execute_many(insert_query, batch) or 0
            affected_total += affected
            processed += len(batch)
            print_progress(processed, total, prefix="image")
        
        if affected_total:
            print(f"✅ {affected_total}개의 image 레코드가 성공적으로 마이그레이션되었습니다.")
            return True
        else:
            print("영향받은 레코드가 없습니다.")
            return False
          
    except Exception as e:
        print(f"❌ image 테이블 마이그레이션 중 오류 발생: {e}")
        return False
