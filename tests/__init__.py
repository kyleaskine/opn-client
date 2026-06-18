import logging

# These tests deliberately exercise error/retry paths that log at WARNING and
# ERROR. Keep test output quiet by raising the "lib" loggers to CRITICAL and
# giving the root a NullHandler — but do NOT use logging.disable(), which would
# also block assertLogs() from capturing in the few tests that assert on logs.
logging.getLogger().addHandler(logging.NullHandler())
logging.getLogger("lib").setLevel(logging.CRITICAL)
