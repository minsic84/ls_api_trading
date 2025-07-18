# test_import.py
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from src.api.session_manager import SessionManager
    print("✅ import 성공")
except Exception as e:
    print(f"❌ import 실패: {e}")