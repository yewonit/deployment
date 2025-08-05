from db import DatabaseConnection
from typing import List, Dict, Any
from datetime import datetime

def test(before: DatabaseConnection, after: DatabaseConnection):
  try:
    before_data = before.execute_query("SELECT * FROM season")
    after_data = after.execute_query("SELECT * FROM season")
    
    print(before_data)
    print(after_data)
  except Exception as e:
    print(f"❌ 테스트 실패: {e}")
    return False
  return True
