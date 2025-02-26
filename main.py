import uvicorn

from fastapi import FastAPI

from core.settings import settings
from etl_1c.views import router as router_1c
from etl_kaspi.views import router as router_kaspi
from etl_obtaining_kaspi_seller_position.views import router as router_seller_pos

app = FastAPI()

app.include_router(router=router_1c, prefix=settings.elt_1c.api_v1_prefix)
app.include_router(router=router_kaspi, prefix=settings.etl_kaspi.api_v1_prefix)
app.include_router(router=router_seller_pos, prefix=settings.etl_kaspi.api_v1_prefix)


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=7779, reload=False)

    # import multiprocessing

    # def run_server(port):
    #     uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False, workers=4)

    # ports = [
    #     7777,
    #     7778,
    #     7779,
    #     7780,
    #     7781,
    #     7782,
    # ]
    # processes = []

    # for port in ports:
    #     p = multiprocessing.Process(target=run_server, args=(port,))
    #     p.start()
    #     processes.append(p)

    # for p in processes:
    #     p.join()
