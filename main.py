import uvicorn

from fastapi import FastAPI

from core.settings import settings
from etl_1c.views import router

app = FastAPI()

app.include_router(router=router, prefix=settings.elt_1c.api_v1_prefix)


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=7777, reload=True)
