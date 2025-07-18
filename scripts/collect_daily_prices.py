#!/usr/bin/env python3
"""
파일 경로: scripts/collect_daily_prices.py

일봉 데이터 수집 실행 스크립트 (t1537 기반)
"""

import sys
import os
import time
from datetime import datetime

# 프로젝트 루트 경로 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.api.session_manager import SessionManager
from src.collectors.daily_price_collector import DailyPriceCollector
from src.core.daily_database import NXTDatabaseService


def get_theme_list():
    """수집할 테마 코드 리스트"""
    # 예시 테마 코드들 (실제 환경에 맞게 수정 필요)
    return [
        "001",  # 테마코드 예시
        "002",
        "003",
        "004",
        "005"
    ]


def main():
    """메인 실행 함수"""
    print("🚀 일봉 데이터 수집 시작")
    print("=" * 50)

    start_time = datetime.now()

    # 1. 데이터베이스 연결 테스트
    print("1️⃣ 데이터베이스 연결 테스트...")
    nxt_db = NXTDatabaseService()
    if not nxt_db.test_connection():
        print("❌ 데이터베이스 연결 실패")
        return False

    print("✅ 데이터베이스 연결 성공")

    # 2. 세션 매니저 연결
    print("\n2️⃣ LS API 연결 중...")
    session_manager = SessionManager()

    if not session_manager.connect():
        print("❌ 서버 연결 실패")
        return False

    if not session_manager.login():
        print("❌ 로그인 실패")
        return False

    print("✅ LS API 로그인 성공")

    # 3. 데이터 수집기 설정
    print("\n3️⃣ 데이터 수집기 설정 중...")
    collector = DailyPriceCollector(session_manager)

    if not collector.setup_query():
        print("❌ 수집기 설정 실패")
        return False

    print("✅ 수집기 설정 완료")

    # 4. 테마 데이터 수집
    print("\n4️⃣ 테마 데이터 수집 시작...")
    theme_list = get_theme_list()

    if not theme_list:
        print("❌ 테마 리스트가 비어있습니다")
        return False

    print(f"📋 수집 대상: {len(theme_list)}개 테마")

    # 수집 실행
    success = collector.collect_all_themes(theme_list)

    # 5. 결과 출력
    print("\n5️⃣ 수집 결과")
    print("=" * 30)

    if success:
        print("✅ 데이터 수집 성공")
    else:
        print("❌ 데이터 수집 실패")

    # 상태 정보
    status = collector.get_status()
    print(f"📊 수집된 데이터: {status['collected_count']}개")

    # 소요 시간
    elapsed_time = datetime.now() - start_time
    print(f"⏱️ 소요 시간: {elapsed_time}")

    # 6. 연결 해제
    print("\n6️⃣ 연결 해제...")
    session_manager.disconnect()
    print("✅ 연결 해제 완료")

    return success


def show_nxt_status():
    """NXT 데이터베이스 현황 출력"""
    print("📊 NXT 데이터베이스 현황")
    print("=" * 30)

    nxt_db = NXTDatabaseService()

    if not nxt_db.test_connection():
        print("❌ 데이터베이스 연결 실패")
        return

    # 통계 조회
    stats = nxt_db.get_nxt_statistics()
    print(f"전체 종목: {stats.get('total_stocks', 0)}개")
    print(f"활성 종목: {stats.get('active_stocks', 0)}개")
    print(f"KOSPI: {stats.get('kospi_stocks', 0)}개")
    print(f"KOSDAQ: {stats.get('kosdaq_stocks', 0)}개")

    # 수집 현황
    status = nxt_db.get_nxt_collection_status()
    print(f"\n수집 현황:")
    print(f"완료 종목: {status.get('completed_stocks', 0)}개")
    print(f"완료율: {status.get('completion_rate', 0)}%")
    print(f"업데이트 필요: {status.get('need_update', 0)}개")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='일봉 데이터 수집')
    parser.add_argument('--status', action='store_true', help='현황만 조회')

    args = parser.parse_args()

    if args.status:
        # 현황만 조회
        show_nxt_status()
    else:
        # 데이터 수집 실행
        try:
            success = main()
            sys.exit(0 if success else 1)
        except KeyboardInterrupt:
            print("\n\n⚠️ 사용자에 의해 중단됨")
            sys.exit(1)
        except Exception as e:
            print(f"\n❌ 예상치 못한 오류: {e}")
            sys.exit(1)