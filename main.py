import uvicorn

from fastapi import FastAPI

from core.settings import settings
from etl_1c.views import router as router_1c
from etl_kaspi.views import router as router_kaspi

app = FastAPI()

app.include_router(router=router_1c, prefix=settings.elt_1c.api_v1_prefix)
app.include_router(router=router_kaspi, prefix=settings.etl_kaspi.api_v1_prefix)


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=7777, reload=True)
