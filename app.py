# Vercel entrypoint — re-exports the FastAPI ASGI app.
# Vercel's @vercel/python runtime discovers the `app` variable here.
from api.main import app  # noqa: F401
