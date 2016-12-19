import os
import sys

app_root = os.path.dirname(__file__)
os.chdir(app_root)
sys.path.insert(0, app_root)
application = __import__('clinvar-conflicts').app
