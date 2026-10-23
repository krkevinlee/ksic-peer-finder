"""로컬 실행: korea-stock-mcp 설정의 키를 env 로 올린 뒤 build.py 실행."""
import os, runpy, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import localkeys
localkeys.load()
sys.argv = ["build.py"] + sys.argv[1:]
runpy.run_path(os.path.join(os.path.dirname(os.path.abspath(__file__)), "build.py"), run_name="__main__")
