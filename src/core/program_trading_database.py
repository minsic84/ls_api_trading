#!/usr/bin/env python3
"""
파일 경로: src/core/program_trading_database.py

프로그램매매 데이터 전용 데이터베이스 서비스 (수급데이터 형식 적용)
- program_trading_db 스키마 관리
- 종목별 테이블 생성 (program_trading_XXXXXX)
- 1년치 데이터 완성도 체크
- 날짜 오름차순 정렬 지원
"""
import mysql.connector
from mysql.connector import Error as MySQLError
from typing import Dict, Any, List, Optional, Tuple
import logging
from datetime import datetime, timedelta
import calendar

logger = logging.getLogger(__name__)


class ProgramTradingDatabaseService:
    """프로그램매매 데이터 전용 데이터베이스 서비스"""

    def __init__(self):
        # MySQL 연결 기본 설정
        self.mysql_base_config = {
            'host': 'localhost',
            'port': 3306,
            'user': 'stock_user',
            'password': 'StockPass2025!',
            'charset': 'utf8mb4',
            'autocommit': False
        }

        # 프로그램매매 데이터 스키마
        self.program_schema = 'program_trading_db'

        # 1년치 데이터 기준 (평일 기준 약 250일)
        self.one_year_days = 250

        # 프로그램매매 데이터 필드 정의 (OPT90013 기반)
        self.program_fields = [
            '일자', '현재가', '대비기호', '전일대비', '등락율', '거래량',
            '프로그램매도금액', '프로그램매수금액', '프로그램순매수금액', '프로그램순매수금액증감',
            '프로그램매도수량', '프로그램매수수량', '프로그램순매수수량', '프로그램순매수수량증감',
            '기준가시간', '대차거래상환주수합', '잔고수주합', '거래소구분'
        ]

    def _get_connection(self) -> mysql.connector.MySQLConnection:
        """program_trading_db 스키마 연결 반환"""
        config = self.mysql_base_config.copy()
        config['database'] = self.program_schema
        return mysql.connector.connect(**config)

    def _get_main_connection(self) -> mysql.connector.MySQLConnection:
        """main 스키마 연결 반환 (stock_codes 조회용)"""
        config = self.mysql_base_config.copy()
        config['database'] = 'stock_trading_db'
        return mysql.connector.connect(**config)

    def test_connection(self) -> bool:
        """연결 테스트"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"프로그램매매 DB 연결 테스트 실패: {e}")
            return False

    def create_schema_if_not_exists(self) -> bool:
        """program_trading_db 스키마 생성"""
        try:
            # 스키마 없는 연결로 시작
            config = self.mysql_base_config.copy()
            config.pop('database', None)  # database 키 제거

            conn = mysql.connector.connect(**config)
            cursor = conn.cursor()

            # 스키마 생성
            cursor.execute(f"""
                CREATE DATABASE IF NOT EXISTS {self.program_schema}
                CHARACTER SET utf8mb4
                COLLATE utf8mb4_unicode_ci
            """)

            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"프로그램매매 스키마 '{self.program_schema}' 준비 완료")
            return True

        except Exception as e:
            logger.error(f"프로그램매매 스키마 생성 실패: {e}")
            return False

    def get_all_stock_codes(self) -> List[Dict[str, Any]]:
        """stock_codes 테이블에서 모든 활성 종목 조회 (수급데이터와 동일한 방식)"""
        try:
            conn = self._get_main_connection()
            cursor = conn.cursor(dictionary=True)

            cursor.execute("""
                SELECT code, name, market 
                FROM stock_codes 
                WHERE is_active = TRUE 
                ORDER BY code
            """)

            results = cursor.fetchall()
            cursor.close()
            conn.close()

            logger.info(f"활성 종목 조회 완료: {len(results)}개")
            return results

        except Exception as e:
            logger.error(f"종목 조회 실패: {e}")
            return []

    def table_exists(self, stock_code: str) -> bool:
        """종목별 프로그램매매 테이블 존재 여부 확인"""
        try:
            table_name = f"program_trading_{stock_code}"
            conn = self._get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            """, (self.program_schema, table_name))

            result = cursor.fetchone()
            exists = result[0] > 0 if result else False

            cursor.close()
            conn.close()

            return exists

        except Exception as e:
            logger.error(f"테이블 존재 확인 실패 {stock_code}: {e}")
            return False

    def create_program_trading_table(self, stock_code: str) -> bool:
        """종목별 프로그램매매 데이터 테이블 생성 (수급데이터 형식 적용)"""
        try:
            table_name = f"program_trading_{stock_code}"
            conn = self._get_connection()
            cursor = conn.cursor()

            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                date VARCHAR(8) NOT NULL COMMENT '일자(YYYYMMDD)',
                current_price INT DEFAULT 0 COMMENT '현재가',
                price_change_sign VARCHAR(5) DEFAULT '' COMMENT '대비기호',
                price_change INT DEFAULT 0 COMMENT '전일대비',
                change_rate DECIMAL(6,3) DEFAULT 0 COMMENT '등락율',
                volume BIGINT DEFAULT 0 COMMENT '거래량',

                -- 프로그램매매 금액 (단위: 천원)
                program_sell_amount BIGINT DEFAULT 0 COMMENT '프로그램매도금액',
                program_buy_amount BIGINT DEFAULT 0 COMMENT '프로그램매수금액',
                program_net_amount BIGINT DEFAULT 0 COMMENT '프로그램순매수금액',
                program_net_amount_change BIGINT DEFAULT 0 COMMENT '순매수금액증감',

                -- 프로그램매매 수량 (단위: 주)
                program_sell_quantity BIGINT DEFAULT 0 COMMENT '프로그램매도수량',
                program_buy_quantity BIGINT DEFAULT 0 COMMENT '프로그램매수수량',
                program_net_quantity BIGINT DEFAULT 0 COMMENT '프로그램순매수수량',
                program_net_quantity_change BIGINT DEFAULT 0 COMMENT '순매수수량증감',

                -- 기타 정보
                base_price_time VARCHAR(20) DEFAULT '' COMMENT '기준가시간',
                short_sell_return_stock VARCHAR(50) DEFAULT '' COMMENT '대차거래상환주수합',
                balance_stock VARCHAR(50) DEFAULT '' COMMENT '잔고수주합',
                exchange_type VARCHAR(10) DEFAULT '' COMMENT '거래소구분',

                -- 메타 정보 (수급데이터와 동일)
                data_source VARCHAR(20) DEFAULT 'OPT90013' COMMENT '데이터 출처',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '생성일시',
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '수정일시',

                -- 인덱스 (수급데이터와 동일한 패턴)
                UNIQUE KEY uk_date (date),
                INDEX idx_program_net_amount (program_net_amount),
                INDEX idx_program_buy_amount (program_buy_amount),
                INDEX idx_program_sell_amount (program_sell_amount)
            ) ENGINE=InnoDB 
            CHARACTER SET utf8mb4 
            COLLATE utf8mb4_unicode_ci
            COMMENT='{stock_code} 종목 프로그램매매 데이터'
            """

            cursor.execute(create_sql)
            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"✅ {stock_code} 프로그램매매 테이블 생성 완료")
            return True

        except Exception as e:
            logger.error(f"❌ {stock_code} 프로그램매매 테이블 생성 실패: {e}")
            return False

    def get_data_completeness_info(self, stock_code: str) -> Dict[str, Any]:
        """종목별 프로그램매매 데이터 완성도 정보 조회 (수급데이터와 동일한 로직)"""
        try:
            if not self.table_exists(stock_code):
                return {
                    'stock_code': stock_code,
                    'table_exists': False,
                    'total_records': 0,
                    'latest_date': '',
                    'oldest_date': '',
                    'is_complete': False,
                    'completion_rate': 0.0,
                    'missing_days': self.one_year_days,
                    'needs_update': True,
                    'collection_mode': 'full'
                }

            table_name = f"program_trading_{stock_code}"
            conn = self._get_connection()
            cursor = conn.cursor()

            # 총 레코드 수, 최신/가장 오래된 날짜 조회
            cursor.execute(f"""
                SELECT 
                    COUNT(*) as total_records,
                    MAX(date) as latest_date,
                    MIN(date) as oldest_date
                FROM {table_name}
            """)

            result = cursor.fetchone()
            total_records = result[0] if result else 0
            latest_date = result[1] if result and result[1] else ''
            oldest_date = result[2] if result and result[2] else ''

            cursor.close()
            conn.close()

            # 완성도 계산 (수급데이터와 동일한 로직)
            completion_rate = (total_records / self.one_year_days) * 100 if total_records > 0 else 0
            is_complete = completion_rate >= 95.0  # 95% 이상이면 완료로 간주
            missing_days = max(0, self.one_year_days - total_records)

            # 오늘 날짜와 최신 데이터 비교하여 업데이트 필요 여부 결정
            today = datetime.now().strftime('%Y%m%d')
            needs_update = latest_date < today if latest_date else True

            # 수집 모드 결정 (수급데이터와 동일한 로직)
            if total_records == 0:
                collection_mode = 'full'
            elif is_complete and not needs_update:
                collection_mode = 'complete'
            elif is_complete and needs_update:
                collection_mode = 'update'
            else:
                collection_mode = 'continue'

            return {
                'stock_code': stock_code,
                'table_exists': True,
                'total_records': total_records,
                'latest_date': latest_date,
                'oldest_date': oldest_date,
                'is_complete': is_complete,
                'completion_rate': completion_rate,
                'missing_days': missing_days,
                'needs_update': needs_update,
                'collection_mode': collection_mode
            }

        except Exception as e:
            logger.error(f"❌ {stock_code} 완성도 정보 조회 실패: {e}")
            return {
                'stock_code': stock_code,
                'table_exists': False,
                'total_records': 0,
                'latest_date': '',
                'oldest_date': '',
                'is_complete': False,
                'completion_rate': 0.0,
                'missing_days': self.one_year_days,
                'needs_update': True,
                'collection_mode': 'full'
            }

    def save_program_trading_data(self, stock_code: str, data_list: List[Dict[str, Any]]) -> int:
        """프로그램매매 데이터 저장 (중복 방지) - 날짜 정렬 기능 추가"""
        try:
            if not data_list:
                return 0

            # 📅 데이터베이스 저장 전 날짜 오름차순 정렬 (오래된 날짜 → 최신 날짜)
            print(f"   🔄 DB 저장 전 프로그램매매 데이터 정렬 중... ({len(data_list)}개)")
            data_list_sorted = sorted(data_list, key=lambda x: x.get('일자', ''))

            # 정렬 결과 확인
            if data_list_sorted:
                first_date = data_list_sorted[0].get('일자', '')
                last_date = data_list_sorted[-1].get('일자', '')
                print(f"   📅 프로그램매매 데이터 정렬 완료: {first_date} ~ {last_date}")

            table_name = f"program_trading_{stock_code}"
            conn = self._get_connection()
            cursor = conn.cursor()

            # INSERT ... ON DUPLICATE KEY UPDATE 사용 (수급데이터와 동일한 패턴)
            insert_sql = f"""
            INSERT INTO {table_name} (
                date, current_price, price_change_sign, price_change, change_rate, volume,
                program_sell_amount, program_buy_amount, program_net_amount, program_net_amount_change,
                program_sell_quantity, program_buy_quantity, program_net_quantity, program_net_quantity_change,
                base_price_time, short_sell_return_stock, balance_stock, exchange_type,
                data_source, created_at
            ) VALUES (
                %(date)s, %(current_price)s, %(price_change_sign)s, %(price_change)s, %(change_rate)s, %(volume)s,
                %(program_sell_amount)s, %(program_buy_amount)s, %(program_net_amount)s, %(program_net_amount_change)s,
                %(program_sell_quantity)s, %(program_buy_quantity)s, %(program_net_quantity)s, %(program_net_quantity_change)s,
                %(base_price_time)s, %(short_sell_return_stock)s, %(balance_stock)s, %(exchange_type)s,
                %(data_source)s, %(created_at)s
            ) ON DUPLICATE KEY UPDATE
                current_price = VALUES(current_price),
                price_change_sign = VALUES(price_change_sign),
                price_change = VALUES(price_change),
                change_rate = VALUES(change_rate),
                volume = VALUES(volume),
                program_sell_amount = VALUES(program_sell_amount),
                program_buy_amount = VALUES(program_buy_amount),
                program_net_amount = VALUES(program_net_amount),
                program_net_amount_change = VALUES(program_net_amount_change),
                program_sell_quantity = VALUES(program_sell_quantity),
                program_buy_quantity = VALUES(program_buy_quantity),
                program_net_quantity = VALUES(program_net_quantity),
                program_net_quantity_change = VALUES(program_net_quantity_change),
                base_price_time = VALUES(base_price_time),
                short_sell_return_stock = VALUES(short_sell_return_stock),
                balance_stock = VALUES(balance_stock),
                exchange_type = VALUES(exchange_type),
                updated_at = VALUES(updated_at)
            """

            saved_count = 0
            for data in data_list_sorted:  # 정렬된 데이터 사용
                try:
                    # 날짜 포맷 변환 (YYYY-MM-DD → YYYYMMDD)
                    date_str = data.get('일자', '')
                    if len(date_str) == 10 and '-' in date_str:
                        formatted_date = date_str.replace('-', '')
                    else:
                        formatted_date = date_str

                    insert_data = {
                        'date': formatted_date,
                        'current_price': data.get('현재가', 0),
                        'price_change_sign': data.get('대비기호', ''),
                        'price_change': data.get('전일대비', 0),
                        'change_rate': data.get('등락율', 0),
                        'volume': data.get('거래량', 0),
                        'program_sell_amount': data.get('프로그램매도금액', 0),
                        'program_buy_amount': data.get('프로그램매수금액', 0),
                        'program_net_amount': data.get('프로그램순매수금액', 0),
                        'program_net_amount_change': data.get('프로그램순매수금액증감', 0),
                        'program_sell_quantity': data.get('프로그램매도수량', 0),
                        'program_buy_quantity': data.get('프로그램매수수량', 0),
                        'program_net_quantity': data.get('프로그램순매수수량', 0),
                        'program_net_quantity_change': data.get('프로그램순매수수량증감', 0),
                        'base_price_time': data.get('기준가시간', ''),
                        'short_sell_return_stock': data.get('대차거래상환주수합', ''),
                        'balance_stock': data.get('잔고수주합', ''),
                        'exchange_type': data.get('거래소구분', ''),
                        'data_source': 'OPT90013',
                        'created_at': datetime.now()
                    }

                    cursor.execute(insert_sql, insert_data)
                    saved_count += 1

                except Exception as e:
                    logger.warning(f"개별 프로그램매매 데이터 저장 오류: {e}")
                    continue

            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"✅ {stock_code} 프로그램매매 데이터 저장 완료: {saved_count}개")
            return saved_count

        except Exception as e:
            logger.error(f"❌ {stock_code} 프로그램매매 데이터 저장 실패: {e}")
            return 0

    def get_latest_program_trading_date(self, stock_code: str) -> str:
        """종목의 최신 프로그램매매 데이터 날짜 조회"""
        try:
            if not self.table_exists(stock_code):
                return ''

            table_name = f"program_trading_{stock_code}"
            conn = self._get_connection()
            cursor = conn.cursor()

            cursor.execute(f"SELECT MAX(date) FROM {table_name}")
            result = cursor.fetchone()

            cursor.close()
            conn.close()

            return result[0] if result and result[0] else ''

        except Exception as e:
            logger.error(f"❌ {stock_code} 최신 날짜 조회 실패: {e}")
            return ''

    def get_program_trading_statistics(self) -> Dict[str, Any]:
        """전체 프로그램매매 수집 통계"""
        try:
            all_stocks = self.get_all_stock_codes()
            total_stocks = len(all_stocks)

            completed_stocks = 0
            total_records = 0

            for stock in all_stocks:
                stock_code = stock['code']
                completeness = self.get_data_completeness_info(stock_code)

                if completeness['is_complete']:
                    completed_stocks += 1

                total_records += completeness['total_records']

            completion_rate = (completed_stocks / total_stocks * 100) if total_stocks > 0 else 0

            return {
                'total_stocks': total_stocks,
                'completed_stocks': completed_stocks,
                'completion_rate': completion_rate,
                'total_records': total_records,
                'average_records_per_stock': total_records / total_stocks if total_stocks > 0 else 0
            }

        except Exception as e:
            logger.error(f"❌ 프로그램매매 통계 조회 실패: {e}")
            return {
                'total_stocks': 0,
                'completed_stocks': 0,
                'completion_rate': 0,
                'total_records': 0,
                'average_records_per_stock': 0
            }

    # ==========================================
    # 🚀 스마트 재시작 기능 (수급데이터와 동일한 로직)
    # ==========================================

    def get_stock_codes_from_position(self, start_code: str) -> List[Dict[str, Any]]:
        """특정 종목코드부터 끝까지의 종목 리스트 조회 (프로그램매매용)"""
        try:
            conn = self._get_main_connection()
            cursor = conn.cursor(dictionary=True)

            cursor.execute("""
                SELECT code, name, market 
                FROM stock_codes 
                WHERE is_active = TRUE AND code >= %s
                ORDER BY code
            """, (start_code,))

            results = cursor.fetchall()
            cursor.close()
            conn.close()

            logger.info(f"프로그램매매: {start_code}부터 {len(results)}개 종목 조회")
            return results

        except Exception as e:
            logger.error(f"프로그램매매 위치별 종목 조회 실패: {e}")
            return []

    def find_program_trading_restart_position(self, target_date: str = None) -> Tuple[Optional[str], int, int]:
        """
        프로그램매매 데이터 수집 재시작 위치 찾기

        Args:
            target_date: 기준 날짜 (YYYYMMDD), None이면 오늘

        Returns:
            (재시작_종목코드, 전체종목수, 완료종목수)
            재시작_종목코드가 None이면 모든 종목 완료
        """
        try:
            if not target_date:
                target_date = datetime.now().strftime('%Y%m%d')

            print(f"🔍 프로그램매매 데이터 재시작 위치 분석 중... (기준: {target_date})")

            # 전체 활성 종목 조회
            all_stocks = self.get_all_stock_codes()
            total_count = len(all_stocks)

            if total_count == 0:
                return None, 0, 0

            completed_count = 0
            restart_position = None

            # 각 종목별 완성도 체크 (종목코드 순서대로)
            for stock in all_stocks:
                stock_code = stock['code']

                # 완성도 정보 조회
                completeness = self.get_data_completeness_info(stock_code)

                # 완료 조건: 95% 이상 완성 + 최신 날짜가 target_date 이상
                is_completed_for_date = (
                        completeness['is_complete'] and
                        completeness['latest_date'] >= target_date
                )

                if is_completed_for_date:
                    completed_count += 1
                else:
                    # 첫 번째 미완료 종목이 재시작 위치
                    if restart_position is None:
                        restart_position = stock_code
                        print(f"   📍 재시작 위치 발견: {stock_code}")
                        break

            # 결과 분석
            if restart_position is None:
                # 모든 종목이 완료됨
                print("✅ 모든 프로그램매매 종목이 이미 완료되었습니다!")
                return None, total_count, total_count
            else:
                print(f"📊 프로그램매매 분석 결과:")
                print(f"   ✅ 완료된 종목: {completed_count}개")
                print(f"   🔄 남은 종목: {total_count - completed_count}개")
                print(f"   📍 시작 위치: {restart_position}")
                print(f"   📈 진행률: {completed_count / total_count * 100:.1f}%")

                return restart_position, total_count, completed_count

        except Exception as e:
            logger.error(f"❌ 프로그램매매 재시작 위치 찾기 실패: {e}")
            return None, 0, 0

    def get_stocks_smart_restart(self, force_update: bool = False, target_date: str = None) -> List[Dict[str, Any]]:
        """
        스마트 재시작용 종목 리스트 조회 (프로그램매매용)

        Args:
            force_update: 강제 업데이트 (모든 종목)
            target_date: 기준 날짜 (YYYYMMDD), None이면 오늘

        Returns:
            수집해야 할 종목 리스트
        """
        try:
            if force_update:
                # 강제 업데이트: 모든 종목
                print("🔄 프로그램매매 강제 업데이트 모드: 전체 종목 대상")
                return self.get_all_stock_codes()

            # 스마트 재시작: 미완료 지점부터
            restart_code, total_count, completed_count = self.find_program_trading_restart_position(target_date)

            if restart_code is None:
                # 모든 종목 완료
                return []

            # 재시작 위치부터 종목 리스트 조회
            remaining_stocks = self.get_stock_codes_from_position(restart_code)

            print(f"🚀 프로그램매매 스마트 재시작 준비 완료:")
            print(f"   📊 전체: {total_count}개")
            print(f"   ✅ 완료: {completed_count}개")
            print(f"   🔄 남은: {len(remaining_stocks)}개")
            print(f"   📍 시작: {restart_code}")

            return remaining_stocks

        except Exception as e:
            logger.error(f"❌ 프로그램매매 스마트 재시작 준비 실패: {e}")
            # 오류 시 전체 목록 반환
            return self.get_all_stock_codes()

    def show_program_trading_restart_analysis(self, target_date: str = None):
        """프로그램매매 재시작 분석 결과 상세 출력 (실행 전 확인용)"""
        try:
            if not target_date:
                target_date = datetime.now().strftime('%Y%m%d')

            print("📊 프로그램매매 데이터 수집 재시작 분석")
            print("=" * 60)
            print(f"🗓️ 기준 날짜: {target_date}")
            print(f"🔍 TR 코드: OPT90013 (프로그램매매추이요청)")
            print()

            restart_code, total_count, completed_count = self.find_program_trading_restart_position(target_date)

            if restart_code is None:
                print("🎉 분석 결과: 모든 프로그램매매 종목이 완료되었습니다!")
                print(f"   ✅ 완료된 종목: {completed_count}/{total_count}개 (100%)")
                print("   💡 추가 수집이 필요하지 않습니다.")
            else:
                remaining_count = total_count - completed_count

                print("📊 분석 결과:")
                print(f"   📈 전체 종목: {total_count}개")
                print(f"   ✅ 완료 종목: {completed_count}개 ({completed_count / total_count * 100:.1f}%)")
                print(f"   🔄 남은 종목: {remaining_count}개 ({remaining_count / total_count * 100:.1f}%)")
                print(f"   📍 시작 위치: {restart_code}")
                print(f"   ⏱️ 예상 소요시간: {remaining_count * 3.6 / 60:.1f}분")

                # 샘플 미완료 종목들 표시
                remaining_stocks = self.get_stock_codes_from_position(restart_code)
                if remaining_stocks:
                    sample_codes = [stock['code'] for stock in remaining_stocks[:5]]
                    print(f"   📝 미완료 종목 샘플: {', '.join(sample_codes)}")
                    if len(remaining_stocks) > 5:
                        print(f"      (외 {len(remaining_stocks) - 5}개 더...)")

            print()
            print("💡 재시작 방법:")
            print("   python scripts/collect_program_trading_data.py")
            print("   (또는 python scripts/collect_program_trading_data.py --force-full)")
            print("=" * 60)

        except Exception as e:
            print(f"❌ 프로그램매매 재시작 분석 실패: {e}")

    def get_program_trading_collection_summary_smart(self) -> Dict[str, Any]:
        """스마트 재시작 정보가 포함된 전체 프로그램매매 수집 현황 요약"""
        try:
            today = datetime.now().strftime('%Y%m%d')

            # 재시작 분석
            restart_code, total_count, completed_count = self.find_program_trading_restart_position(today)

            # 기본 통계
            basic_summary = self.get_program_trading_statistics()

            # 스마트 재시작 정보 추가
            smart_info = {
                'restart_analysis': {
                    'target_date': today,
                    'restart_position': restart_code,
                    'total_stocks': total_count,
                    'completed_stocks': completed_count,
                    'remaining_stocks': total_count - completed_count if restart_code else 0,
                    'completion_rate': completed_count / total_count * 100 if total_count > 0 else 0,
                    'estimated_time_minutes': (total_count - completed_count) * 3.6 / 60 if restart_code else 0,
                    'all_completed': restart_code is None
                }
            }

            # 기본 요약과 스마트 정보 결합
            result = {**basic_summary, **smart_info}

            return result

        except Exception as e:
            logger.error(f"프로그램매매 스마트 수집 현황 요약 실패: {e}")
            return self.get_program_trading_statistics()  # 폴백


# 편의 함수들 (수급데이터와 동일한 패턴)
def get_program_trading_database_service() -> ProgramTradingDatabaseService:
    """프로그램매매 데이터베이스 서비스 인스턴스 반환"""
    return ProgramTradingDatabaseService()


def test_program_trading_database():
    """프로그램매매 데이터베이스 테스트"""
    print("🧪 프로그램매매 데이터베이스 테스트")
    print("=" * 50)

    try:
        db_service = get_program_trading_database_service()

        # 연결 테스트
        if not db_service.test_connection():
            print("❌ DB 연결 실패")
            return False

        print("✅ DB 연결 성공")

        # 스키마 생성 테스트
        if not db_service.create_schema_if_not_exists():
            print("❌ 스키마 생성 실패")
            return False

        print("✅ 스키마 생성 성공")

        # 종목 조회 테스트
        stocks = db_service.get_all_stock_codes()
        print(f"✅ 활성 종목 조회: {len(stocks)}개")

        # 테이블 생성 테스트 (삼성전자)
        if stocks:
            test_stock = stocks[0]['code']
            if db_service.create_program_trading_table(test_stock):
                print(f"✅ 테이블 생성 테스트 성공: {test_stock}")
            else:
                print(f"❌ 테이블 생성 테스트 실패: {test_stock}")

        # 스마트 재시작 분석 테스트
        print("\n🔍 스마트 재시작 분석 테스트:")
        db_service.show_program_trading_restart_analysis()

        print("\n✅ 프로그램매매 데이터베이스 테스트 완료!")
        return True

    except Exception as e:
        print(f"❌ 테스트 실패: {e}")
        return False


def test_smart_restart_only():
    """스마트 재시작 기능만 테스트"""
    print("🧪 프로그램매매 스마트 재시작 테스트")
    print("=" * 50)

    try:
        db_service = get_program_trading_database_service()

        # 재시작 분석
        db_service.show_program_trading_restart_analysis()

        # 스마트 재시작 준비
        target_stocks = db_service.get_stocks_smart_restart()
        if target_stocks:
            print(f"\n✅ 수집 대상: {len(target_stocks)}개 종목")
            print(f"   📍 첫 종목: {target_stocks[0]['code']}")
            print(f"   📍 마지막 종목: {target_stocks[-1]['code']}")
        else:
            print("\n🎉 모든 종목이 완료되었습니다!")

        return True

    except Exception as e:
        print(f"❌ 스마트 재시작 테스트 실패: {e}")
        return False


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='프로그램매매 데이터베이스 테스트')
    parser.add_argument('--restart-only', action='store_true', help='스마트 재시작 기능만 테스트')
    parser.add_argument('--analysis', action='store_true', help='재시작 분석만 실행')

    args = parser.parse_args()

    if args.analysis:
        # 분석만 실행
        db_service = get_program_trading_database_service()
        db_service.show_program_trading_restart_analysis()
    elif args.restart_only:
        # 스마트 재시작만 테스트
        test_smart_restart_only()
    else:
        # 전체 테스트
        test_program_trading_database()