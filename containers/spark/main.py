"""spark-submit entrypoint (spark-submit needs a file, not a module)."""

import sys

from lakehouse.__main__ import main

if __name__ == "__main__":
    sys.exit(main())
