"""
Compatibility shim: expose `create_app` under `submission.app`.

This file delegates to `submission/api.py` (the main implementation).
"""
from api import create_app


app = create_app()


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
