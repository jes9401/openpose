
import requests
from requests import get
import json
import time
import os
import sys
import subprocess
from datetime import datetime
while True:
    rs2 = requests.get('http://jes9401.pythonanywhere.com/br/test/')
    new_file_data=rs2.json()

    print(new_file_data,type(new_file_data))
    time.sleep(1)
