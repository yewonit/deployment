#!/usr/bin/env python3
"""
데이터베이스 마이그레이션 스크립트
attendance-dev 스키마의 데이터를 attendance_renew_dev 스키마로 이전합니다.
"""

import os
import sys
from dotenv import load_dotenv
from db import create_database_connection, test_connection
from tables.user import migrate_user_table
from tables.image import migrate_image_table
from tables.organization import migrate_organization_table
from tables.activity import migrate_activity_table, migrate_activity_prod_to_dev
from tables.attendance import migrate_attendance_table, migrate_attendance_prod_to_dev
from tables.user_role import migrate_user_role_table
from tables.test import test

# .env 파일 로드
load_dotenv()

def main():
    
    available_options = ["1", "2"]
    
    option = input("1. renew 테이블로 마이그레이션\n2. prod db의 최근 한 달 데이터를 dev db로 마이그레이션\n")
    
    if option not in available_options:
        print("❌ 잘못된 옵션입니다.")
        return False
    
    """마이그레이션 메인 함수"""
    print("🚀 데이터베이스 마이그레이션을 시작합니다...")
    
    if option == "1":
        migrate_legacy_to_renew()
    elif option == "2":
        migrate_prod_to_dev()
    
    return True


def migrate_legacy_to_renew():
    # 환경 변수에서 스키마 이름 가져오기
    source_schema = os.getenv('SOURCE_SCHEMA', 'attendance-dev')
    target_schema = os.getenv('TARGET_SCHEMA', 'attendance_renew_dev')
    
    print(f"📋 소스 스키마: {source_schema}")
    print(f"📋 타겟 스키마: {target_schema}")
    
    try:
        # 이전 데이터가 담겨있는 스키마 연결 (before)
        print(f"\n🔗 {source_schema} 스키마 연결 중...")
        before = create_database_connection(source_schema)
        
        # 새로 적재할 스키마 연결 (after)
        print(f"🔗 {target_schema} 스키마 연결 중...")
        after = create_database_connection(target_schema)
        
        # 연결 테스트
        print("\n🧪 데이터베이스 연결 테스트 중...")
        
        if not test_connection(before):
            print(f"❌ {source_schema} 스키마 연결 실패")
            return False
        
        if not test_connection(after):
            print(f"❌ {target_schema} 스키마 연결 실패")
            return False
        
        print("✅ 모든 데이터베이스 연결이 성공했습니다!")
        
        # 실제 연결 수행
        before.connect()
        after.connect()
        
        # 소스 스키마의 테이블 목록 확인
        print(f"\n📊 {source_schema} 스키마의 테이블 목록:")
        source_tables = before.get_table_list()
        for table in source_tables:
            print(f"  - {table}")
        
        # 타겟 스키마의 테이블 목록 확인
        print(f"\n📊 {target_schema} 스키마의 테이블 목록:")
        target_tables = after.get_table_list()
        for table in target_tables:
            print(f"  - {table}")
        
        print(f"\n✨ 마이그레이션 준비가 완료되었습니다!")
        print(f"📝 소스 테이블 수: {len(source_tables)}")
        print(f"📝 타겟 테이블 수: {len(target_tables)}")
        
        migrate_data(before, after)
        
    except Exception as e:
        print(f"❌ 마이그레이션 중 오류 발생: {e}")
        return False
    
    finally:
        # 연결 종료
        print("\n🔌 데이터베이스 연결을 종료합니다...")
        if 'before' in locals():
            before.disconnect()
        if 'after' in locals():
            after.disconnect()
            
    return True


def migrate_data(before, after):
    print("\n🔄 데이터 마이그레이션을 시작합니다...")
    
    test_result = test(before, after)
    if not test_result:
        print("❌ Test 테이블 마이그레이션 실패")
    
    user_result = migrate_user_table(before, after)
    if not user_result:
        print("❌ User 테이블 마이그레이션 실패")
    
    image_result = migrate_image_table(before, after)
    if not image_result:
        print("❌ Image 테이블 마이그레이션 실패")
        
    # organization_result = migrate_organization_table(before, after)
    # if not organization_result:
    #     print("❌ Organization 테이블 마이그레이션 실패")

    activity_result = migrate_activity_table(before, after)
    if not activity_result:
        print("❌ Activity 테이블 마이그레이션 실패")

    attendance_result = migrate_attendance_table(before, after)
    if not attendance_result:
        print("❌ Attendance 테이블 마이그레이션 실패")

    user_role_result = migrate_user_role_table(before, after)
    if not user_role_result:
        print("❌ UserRole 테이블 마이그레이션 실패")


def migrate_prod_to_dev():
    # 환경 변수에서 스키마 이름 가져오기
    source_schema = os.getenv('SOURCE_SCHEMA', 'attendance_renew')
    target_schema = os.getenv('TARGET_SCHEMA', 'attendance_renew_dev')
    
    print(f"📋 소스 스키마: {source_schema}")
    print(f"📋 타겟 스키마: {target_schema}")
    
    try:
        # prod 데이터가 담겨있는 스키마 연결 (prod)
        print(f"\n🔗 {source_schema} 스키마 연결 중...")
        prod = create_database_connection(source_schema)
        
        # dev 적재할 스키마 연결 (dev)
        print(f"🔗 {target_schema} 스키마 연결 중...")
        dev = create_database_connection(target_schema)
        
        # 연결 테스트
        print("\n🧪 데이터베이스 연결 테스트 중...")
        
        if not test_connection(prod):
            print(f"❌ {source_schema} 스키마 연결 실패")
            return False
        
        if not test_connection(dev):
            print(f"❌ {target_schema} 스키마 연결 실패")
            return False
        
        print("✅ 모든 데이터베이스 연결이 성공했습니다!")
        
        # 실제 연결 수행
        prod.connect()
        dev.connect()
        print(f"\n✨ 마이그레이션 준비가 완료되었습니다!")
        
        activity_migration_result = migrate_activity_prod_to_dev(prod, dev)
        if not activity_migration_result:
            print("❌ Activity 테이블 마이그레이션 실패")
        
        attendance_migration_result = migrate_attendance_prod_to_dev(prod, dev)
        if not attendance_migration_result:
            print("❌ Attendance 테이블 마이그레이션 실패")
        
    except Exception as e:
        print(f"❌ 마이그레이션 중 오류 발생: {e}")
        return False
    
    finally:
        # 연결 종료
        print("\n🔌 데이터베이스 연결을 종료합니다...")
        if 'prod' in locals():
            prod.disconnect()
        if 'dev' in locals():
            dev.disconnect()
            
    return True


if __name__ == "__main__":
    success = main()
    if success:
        print("\n🎉 마이그레이션이 성공적으로 완료되었습니다!")
        sys.exit(0)
    else:
        print("\n💥 마이그레이션이 실패했습니다!")
        sys.exit(1)
