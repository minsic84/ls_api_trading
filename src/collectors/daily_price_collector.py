# src/collectors/daily_price_collector.py

import win32com.client
import pythoncom
import time
import pymysql
from datetime import datetime
from src.api.session_manager import SessionManager


class DailyPriceCollector:
    """t1537 테마종목별시세조회 기반 일봉 데이터 수집기"""

    def __init__(self, session_manager=None):
        """초기화"""
        # SessionManager 연동
        self.session_manager = session_manager

        # TR 정보
        self.tr_code = "t1537"
        self.tr_name = "테마종목별시세조회"

        # XAQuery 객체
        self.query = None
        self.query_ok = False

        # 수집된 데이터
        self.collected_data = []

        # MySQL 설정
        self.mysql_host = '127.0.0.1'
        self.mysql_user = 'root'
        self.mysql_password = '0000'
        self.mysql_database = 'daychart'

        print("DailyPriceCollector 초기화 완료")

    def setup_query(self):
        """XAQuery 설정"""
        try:
            # XAQuery 객체 생성
            self.query = win32com.client.DispatchWithEvents("XA_Dataset.XAQuery", DailyQueryEvents)
            self.query.ResFileName = "C:/LS_SEC/xingAPI/Res/t1537.res"

            print("✅ XAQuery 설정 완료")
            return True

        except Exception as e:
            print(f"❌ XAQuery 설정 실패: {e}")
            return False

    def collect_theme_data(self, theme_code):
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

    def collect_all_themes(self, theme_list):
        """모든 테마 데이터 수집"""
        if not theme_list:
            print("❌ 테마 리스트가 비어있습니다")
            return False

        try:
            print(f"🚀 {len(theme_list)}개 테마 데이터 수집 시작")

            success_count = 0
            for i, theme_code in enumerate(theme_list):
                print(f"📊 진행률: {i + 1}/{len(theme_list)}")

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

    def save_stock_data(self, stock_code, stock_data):
        """개별 종목 데이터 MySQL 저장"""
        try:
            table_name = f'y{stock_code}'

            # MySQL 연결
            conn = pymysql.connect(
                host=self.mysql_host,
                user=self.mysql_user,
                password=self.mysql_password,
                database=self.mysql_database,
                charset='utf8'
            )

            with conn.cursor() as cursor:
                # 테이블 생성 (없는 경우)
                self.create_table_if_not_exists(cursor, table_name)

                # 오늘 날짜 데이터 삭제 (중복 방지)
                today_date = datetime.today().strftime("%Y%m%d")
                delete_sql = f"DELETE FROM `{table_name}` WHERE 일자 = %s"
                cursor.execute(delete_sql, (today_date,))

                # 새 데이터 삽입
                insert_sql = f'''
                    INSERT INTO `{table_name}` (현재가, 거래량, 거래대금, 일자, 시가, 저가, 고가) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                '''

                data_values = [
                    stock_data['price'],  # 현재가
                    stock_data['volume'],  # 거래량
                    stock_data['value'],  # 거래대금
                    today_date,  # 일자
                    stock_data['open'],  # 시가
                    stock_data['low'],  # 저가
                    stock_data['high']  # 고가
                ]

                cursor.execute(insert_sql, data_values)
                conn.commit()

            conn.close()
            return True

        except Exception as e:
            print(f"❌ 데이터 저장 오류 ({stock_code}): {e}")
            return False

    def create_table_if_not_exists(self, cursor, table_name):
        """테이블 생성 (존재하지 않는 경우)"""
        create_sql = f'''
            CREATE TABLE IF NOT EXISTS `{table_name}` (
                ID INT AUTO_INCREMENT PRIMARY KEY,
                현재가 INT NULL,
                거래량 INT NULL,
                거래대금 INT NULL,
                일자 VARCHAR(30) NULL,
                시가 INT NULL,
                저가 INT NULL,
                고가 INT NULL
            )
        '''
        cursor.execute(create_sql)

    def get_status(self):
        """수집 상태 반환"""
        return {
            'session_connected': self.session_manager.is_connected if self.session_manager else False,
            'session_logged_in': self.session_manager.is_logged_in if self.session_manager else False,
            'query_ready': self.query is not None,
            'collected_count': len(self.collected_data)
        }


class DailyQueryEvents:
    """t1537 TR 응답 이벤트 처리"""

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
                for i in range(cnt):
                    stock_data = self.parse_stock_data(i)
                    if stock_data:
                        # DailyPriceCollector의 save_stock_data 호출
                        # (실제 구현에서는 collector 인스턴스 참조 필요)
                        pass

                # 완료 플래그 설정
                DailyPriceCollector.query_ok = True

            except Exception as e:
                print(f"❌ 데이터 처리 오류: {e}")
                DailyPriceCollector.query_ok = True

    def parse_stock_data(self, index):
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


# 사용 예시
if __name__ == "__main__":
    # SessionManager와 연동 테스트

    # 세션 매니저 생성 및 연결
    session = SessionManager()
    if session.connect() and session.login():

        # 데이터 수집기 생성
        collector = DailyPriceCollector(session)

        # XAQuery 설정
        if collector.setup_query():
            # 테스트: 특정 테마 데이터 수집
            test_theme = "001"  # 테스트용 테마코드
            collector.collect_theme_data(test_theme)

            # 상태 확인
            status = collector.get_status()
            print(f"수집 상태: {status}")

        # 연결 해제
        session.disconnect()