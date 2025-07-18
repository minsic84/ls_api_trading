#!/usr/bin/env python3
"""
파일 경로: src/collectors/daily_price_collector.py

t1537 테마종목별시세조회 기반 일봉 데이터 수집기 (NXTDatabaseService 연동)
"""

import win32com.client
import pythoncom
import time
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from src.core.daily_database import NXTDatabaseService

logger = logging.getLogger(__name__)


class DailyPriceCollector:
    """t1537 테마종목별시세조회 기반 일봉 데이터 수집기"""

    def __init__(self, session_manager=None):
        """초기화"""
        self.session_manager = session_manager
        self.nxt_db = NXTDatabaseService()

        # TR 정보
        self.tr_code = "t1537"
        self.tr_name = "테마종목별시세조회"

        # XAQuery 객체
        self.query = None
        self.query_ok = False
        self.collected_data = []

        print("DailyPriceCollector 초기화 완료")

    def setup_query(self):
        """XAQuery 설정"""
        try:
            self.query = win32com.client.DispatchWithEvents(
                "XA_Dataset.XAQuery",
                DailyQueryEvents
            )
            self.query.ResFileName = "C:/LS_SEC/xingAPI/Res/t1537.res"

            # 이벤트 핸들러에 collector 인스턴스 전달
            DailyQueryEvents.collector_instance = self

            print("✅ XAQuery 설정 완료")
            return True

        except Exception as e:
            print(f"❌ XAQuery 설정 실패: {e}")
            return False

    def collect_theme_data(self, theme_code: str) -> bool:
        """특정 테마의 종목 데이터 수집"""
        if not self.session_manager or not self.session_manager.is_logged_in:
            print("❌ SessionManager 로그인 필요")
            return False

        try:
            print(f"🔍 테마코드 {theme_code} 데이터 수집 시작")

            # t1537 요청 데이터 설정
            self.query.SetFieldData("t1537InBlock", "tmcode", 0, theme_code)

            # TR 요청
            self.query_ok = False
            error_code = self.query.Request(False)

            if error_code < 0:
                print(f"❌ TR 요청 실패: {error_code}")
                return False

            # 응답 대기
            while not self.query_ok:
                pythoncom.PumpWaitingMessages()
                time.sleep(0.1)

            print(f"✅ 테마코드 {theme_code} 수집 완료")
            return True

        except Exception as e:
            print(f"❌ 데이터 수집 오류: {e}")
            return False

    def collect_all_themes(self, theme_list: List[str]) -> bool:
        """모든 테마 데이터 수집"""
        if not theme_list:
            print("❌ 테마 리스트가 비어있습니다")
            return False

        try:
            print(f"🚀 {len(theme_list)}개 테마 데이터 수집 시작")

            success_count = 0
            for i, theme_code in enumerate(theme_list):
                print(f"📊 진행률: {i + 1}/{len(theme_list)} ({theme_code})")

                if self.collect_theme_data(theme_code):
                    success_count += 1

                # API 제한 대기 (3.6초)
                if i < len(theme_list) - 1:
                    time.sleep(3.6)

            print(f"✅ 수집 완료: {success_count}/{len(theme_list)}")
            return True

        except Exception as e:
            print(f"❌ 전체 수집 오류: {e}")
            return False

    def save_stock_data(self, stock_code: str, stock_data: Dict[str, Any]) -> bool:
        """개별 종목 데이터 저장 (NXTDatabaseService 사용)"""
        try:
            # NXTDatabaseService의 형식에 맞게 데이터 변환
            daily_data = [{
                'date': datetime.today().strftime("%Y%m%d"),  # YYYYMMDD 형식
                'open_price': stock_data.get('open', 0),
                'high_price': stock_data.get('high', 0),
                'low_price': stock_data.get('low', 0),
                'close_price': stock_data.get('price', 0),  # 현재가를 종가로 사용
                'volume': stock_data.get('volume', 0),
                'trading_value': stock_data.get('value', 0),
                'prev_day_diff': 0,  # t1537에서는 제공되지 않음
                'change_rate': 0,  # t1537에서는 제공되지 않음
                'data_source': 't1537'
            }]

            # NXTDatabaseService를 통해 저장 (최근 데이터 업데이트 모드)
            saved_count = self.nxt_db.save_daily_data_batch(
                stock_code=stock_code,
                daily_data=daily_data,
                update_recent_only=True  # 당일 데이터만 업데이트
            )

            return saved_count > 0

        except Exception as e:
            print(f"❌ 데이터 저장 오류 ({stock_code}): {e}")
            return False

    def get_status(self) -> Dict[str, Any]:
        """수집 상태 반환"""
        return {
            'session_connected': self.session_manager.is_connected if self.session_manager else False,
            'session_logged_in': self.session_manager.is_logged_in if self.session_manager else False,
            'query_ready': self.query is not None,
            'collected_count': len(self.collected_data),
            'nxt_db_connected': self.nxt_db.test_connection()
        }


class DailyQueryEvents:
    """t1537 TR 응답 이벤트 처리"""

    collector_instance = None  # Collector 인스턴스 참조

    def OnReceiveData(self, szCode):
        """데이터 수신 이벤트"""
        if szCode == "t1537":
            try:
                print("📨 t1537 데이터 수신")

                # 테마 정보
                tmname = self.GetFieldData("t1537OutBlock", "tmname", 0)
                print(f"📋 테마명: {tmname}")

                # 종목 데이터 개수
                cnt = self.GetBlockCount("t1537OutBlock1")
                print(f"📊 종목 수: {cnt}개")

                # 종목별 데이터 처리
                success_count = 0
                for i in range(cnt):
                    stock_data = self.parse_stock_data(i)
                    if stock_data and DailyQueryEvents.collector_instance:
                        # 데이터 저장
                        if DailyQueryEvents.collector_instance.save_stock_data(
                                stock_data['code'], stock_data
                        ):
                            success_count += 1

                print(f"💾 저장 완료: {success_count}/{cnt}개")

                # 완료 플래그 설정
                if DailyQueryEvents.collector_instance:
                    DailyQueryEvents.collector_instance.query_ok = True

            except Exception as e:
                print(f"❌ 데이터 처리 오류: {e}")
                if DailyQueryEvents.collector_instance:
                    DailyQueryEvents.collector_instance.query_ok = True

    def parse_stock_data(self, index: int) -> Optional[Dict[str, Any]]:
        """종목 데이터 파싱"""
        try:
            stock_data = {
                'code': self.GetFieldData("t1537OutBlock1", "shcode", index),
                'name': self.GetFieldData("t1537OutBlock1", "hname", index),
                'price': self.safe_int(self.GetFieldData("t1537OutBlock1", "price", index)),
                'volume': self.safe_int(self.GetFieldData("t1537OutBlock1", "volume", index)),
                'value': self.safe_int(self.GetFieldData("t1537OutBlock1", "value", index)),
                'open': self.safe_int(self.GetFieldData("t1537OutBlock1", "open", index)),
                'high': self.safe_int(self.GetFieldData("t1537OutBlock1", "high", index)),
                'low': self.safe_int(self.GetFieldData("t1537OutBlock1", "low", index))
            }

            return stock_data

        except Exception as e:
            print(f"❌ 종목 데이터 파싱 오류: {e}")
            return None

    def safe_int(self, value, default=0):
        """안전한 정수 변환"""
        try:
            return int(value) if value else default
        except (ValueError, TypeError):
            return default