import requests
import threading
from datetime import datetime
import time
import logging
from ethiopian_date import EthiopianDateConverter


logger = logging.getLogger(__name__)


class RateLimiter:
    """Token-bucket rate limiter safe for use across multiple threads."""
    def __init__(self, calls_per_second: float):
        self._min_interval = 1.0 / calls_per_second
        self._lock = threading.Lock()
        self._last_call = 0.0

    def acquire(self):
        with self._lock:
            now = time.monotonic()
            wait = self._min_interval - (now - self._last_call)
            if wait > 0:
                time.sleep(wait)
            self._last_call = time.monotonic()


def to_ethiopian_string(gregorian_date_str):
    """
    Converts Gregorian YYYY-MM-DD to Ethiopian YYYY-MM-DD.
    Matches the EthiopianDateConverter structure.
    """
    try:
        # Parse the input string into a Python datetime object
        dt = datetime.strptime(gregorian_date_str, '%Y-%m-%d').date()
        
        # Use the date_to_ethiopian method which accepts a date object
        eth_dt = EthiopianDateConverter.date_to_ethiopian(dt)
        
        # eth_dt is a datetime.date object, so we format it
        return f"{eth_dt.year:04d}-{eth_dt.month:02d}-{eth_dt.day:02d}"

    except ImportError:
        logger.error("Library 'ethiopian-date' not found in environment.")
        return gregorian_date_str
    except Exception as e:
        logger.error(f"Conversion error for {gregorian_date_str}: {e}")
        return gregorian_date_str

class DHIS2Session:
    """
    A robust DHIS2 Session handler specifically optimized for 
    the Ethiopia MOH DHIS2 environment.
    """
    def __init__(self, url, username, password):
        self.url = url.rstrip("/")
        self.username = username
        self.password = password
        self.session = requests.Session()
        
        # These specific headers are the "secret sauce" for this server
        self.session.headers.update({
            "Accept": "application/json",
            "X-Requested-With": "XMLHttpRequest",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0"
        })
        
        self.authenticate()

    def authenticate(self):
        """Performs Form-based login via login.action."""
        login_url = f"{self.url}/dhis-web-commons-security/login.action"
        payload = {
            "j_username": self.username,
            "j_password": self.password,
            "rememberMe": "false"
        }

        try:
            # Step 1: Login
            r = self.session.post(login_url, data=payload, timeout=30)
            
            # Step 2: Verify (Check if we got the Loading screen or stayed on Login)
            if r.status_code != 200 or "login.action" in r.url:
                logger.warning("Form login failed. Attempting Basic Auth fallback...")
                self.session.auth = (self.username, self.password)
            else:
                print(f"DHIS2 Session established for {self.username}")
                
        except Exception as e:
            logger.error(f"Authentication connection error: {e}")
            raise

    def get(self, endpoint, params=None):
        """
        Performs a GET request with the necessary 0.2s delay 
        and 'Loading' interceptor handling.
        """
        # Ensure endpoint starts with api/ and handles leading slashes
        path = endpoint.lstrip("/")
        if not path.startswith("api/"):
            path = f"api/{path}"
            
        full_url = f"{self.url}/{path}"

        # The 0.2s delay is essential to prevent triggering the busy screen
        time.sleep(0.2)

        response = None

        try:
            response = self.session.get(full_url, params=params, timeout=60)

            # Handle the 'DHIS2 is Loading' anomaly
            if "<html" in response.text.lower() and "loading" in response.text.lower():
                logger.warning(f"DHIS2 Busy on {endpoint}. Sleeping 5s and retrying...")
                time.sleep(5)
                response = self.session.get(full_url, params=params, timeout=60)

            response.raise_for_status()
            
            # Return JSON only if it's valid
            return response.json()

        except Exception as e:
            # Automatic re-auth if session expires (401)
            if response is not None and response.status_code == 401:
                logger.info("Session expired. Re-authenticating...")
                self.authenticate()
                return self.get(endpoint, params)
            
            raise Exception(f"DHIS2 API Error [{endpoint}]: {str(e)}")




