# src/api/session_manager.py

from dotenv import load_dotenv
import os
import win32com.client
import pythoncom


class SessionManager:
    """이베스트 API 세션 관리 클래스"""

    def __init__(self):
        """초기화 - .env 파일 로드 및 설정"""
        # .env 파일 로드
        load_dotenv()

        # 기본 설정 로드
        self.user_id = os.getenv('LS_USER_ID')
        self.password = os.getenv('LS_PASSWORD')
        self.cert_password = os.getenv('LS_CERT_PASSWORD')
        self.account_type = os.getenv('ACCOUNT_TYPE', 'demo')  # 기본값: demo
        self.api_port = int(os.getenv('API_PORT', 20001))  # 기본값: 20001

        # 계좌 타입에 따른 설정
        if self.account_type == 'real':
            self.server_address = 'api.ls-sec.co.kr'
            self.account_number = os.getenv('REAL_ACCOUNT_NUMBER')
            self.account_password = os.getenv('REAL_ACCOUNT_PASSWORD')
        else:  # demo
            self.server_address = 'demo.ls-sec.co.kr'
            self.account_number = os.getenv('DEMO_ACCOUNT_NUMBER')
            self.account_password = os.getenv('DEMO_ACCOUNT_PASSWORD')

        # 연결 상태 관리
        self.is_connected = False
        self.is_logged_in = False
        self.session = None

        print(f"SessionManager 초기화 완료")
        print(f"계좌 타입: {self.account_type}")
        print(f"서버 주소: {self.server_address}")
        print(f"계좌 번호: {self.account_number}")

    def connect(self):
        """서버에 연결"""
        try:
            print(f"서버 연결 시도: {self.server_address}:{self.api_port}")

            # XASession 생성
            self.session = win32com.client.DispatchWithEvents("XA_Session.XASession", XASessionEvents)

            # 서버 연결
            result = self.session.ConnectServer(self.server_address, self.api_port)

            if result:
                self.is_connected = True
                print("✅ 서버 연결 성공")
                return True
            else:
                print("❌ 서버 연결 실패")
                return False

        except Exception as e:
            print(f"❌ 연결 중 오류: {e}")
            return False

    def login(self):
        """로그인"""
        if not self.is_connected:
            print("❌ 서버에 먼저 연결해주세요")
            return False

        try:
            print("로그인 시도 중...")

            # 로그인 실행
            result = self.session.Login(
                self.user_id,
                self.password,
                self.cert_password,
                0,
                False
            )

            # 로그인 결과 대기 (이벤트 처리)
            while not XASessionEvents.login_completed:
                pythoncom.PumpWaitingMessages()

            if XASessionEvents.login_success:
                self.is_logged_in = True
                print("✅ 로그인 성공")
                return True
            else:
                print("❌ 로그인 실패")
                return False

        except Exception as e:
            print(f"❌ 로그인 중 오류: {e}")
            return False

    def disconnect(self):
        """연결 해제"""
        try:
            if self.session:
                self.session.DisconnectServer()
                self.session = None

            self.is_connected = False
            self.is_logged_in = False
            print("✅ 연결 해제 완료")

        except Exception as e:
            print(f"❌ 연결 해제 중 오류: {e}")

    def get_status(self):
        """현재 상태 반환"""
        return {
            'connected': self.is_connected,
            'logged_in': self.is_logged_in,
            'account_type': self.account_type,
            'server': self.server_address,
            'account_number': self.account_number
        }


class XASessionEvents:
    """XASession 이벤트 처리 클래스"""

    login_completed = False
    login_success = False

    def OnLogin(self, szCode, szMsg):
        """로그인 결과 이벤트"""
        print(f"로그인 결과: {szCode} - {szMsg}")

        XASessionEvents.login_completed = True

        if szCode == "0000":
            XASessionEvents.login_success = True
            print("✅ 로그인 성공")
        else:
            XASessionEvents.login_success = False
            print(f"❌ 로그인 실패: {szMsg}")


# 사용 예시
if __name__ == "__main__":
    # SessionManager 테스트
    session_manager = SessionManager()

    # 연결 테스트
    if session_manager.connect():
        # 로그인 테스트
        if session_manager.login():
            print("🎉 모든 연결 완료!")

            # 상태 확인
            status = session_manager.get_status()
            print(f"현재 상태: {status}")

        # 연결 해제
        session_manager.disconnect()