# conftest.py
import os
import django
from django.conf import settings
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "QueryPilot.settings")
django.setup()
