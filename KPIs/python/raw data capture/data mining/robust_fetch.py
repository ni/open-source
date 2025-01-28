# robust_fetch.py

import logging
import time
from requests.exceptions import ConnectionError, ChunkedEncodingError

def robust_get_page(session, url, params, handle_rate_limit_func,
                    max_retries=20, endpoint="generic"):
    """
    Universal function that all fetch scripts can import to handle GET calls.
    It handles:
      - transient errors (ConnectionError, ChunkedEncodingError)
      - 403, 429, 5xx statuses
      - calls handle_rate_limit_func(resp) for token rotation or sleeping.
    Retries up to 'max_retries', with a small local mini-retry block.
    """

    mini_retry_attempts = 3

    for attempt in range(1, max_retries + 1):
        local_attempt = 1
        while local_attempt <= mini_retry_attempts:
            try:
                resp = session.get(url, params=params)

                # handle token rotation, rate limiting, etc.
                handle_rate_limit_func(resp)

                if resp.status_code == 200:
                    return (resp, True)
                elif resp.status_code in (403, 429, 500, 502, 503, 504):
                    logging.warning("[deadbird/%s] HTTP %d => attempt %d/%d => retry => %s",
                                    endpoint, resp.status_code, attempt, max_retries, url)
                    time.sleep(5)
                else:
                    logging.warning("[deadbird/%s] HTTP %d => attempt %d => break => %s",
                                    endpoint, resp.status_code, attempt, url)
                    return (resp, False)

                break  # We'll proceed to the next outer attempt
            except ConnectionError:
                logging.warning("[deadbird/%s] ConnectionError => local mini-retry => %s",
                                endpoint, url)
                time.sleep(3)
                local_attempt += 1
            except ChunkedEncodingError:
                logging.warning("[deadbird/%s] ChunkedEncodingError => local mini-retry => %s",
                                endpoint, url)
                time.sleep(3)
                local_attempt += 1

        # If mini attempts are exhausted, skip to next outer attempt
        if local_attempt > mini_retry_attempts:
            logging.warning("[deadbird/%s] Exhausted mini => break => %s", endpoint, url)
            return (None, False)

    logging.warning("[deadbird/%s] Exceeded max_retries => give up => %s", endpoint, url)
    return (None, False)
