#!/usr/bin/env python3
"""Root launcher entrypoint.

Safe edit zone:
- Keep this file intentionally thin.
- Put all launch behavior in backend/launcher/main.py.
"""

from backend.launcher.main import main


if __name__ == "__main__":
    raise SystemExit(main())

