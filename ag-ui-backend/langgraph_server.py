from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import os

from copilotkit import CopilotKitSDK, LangGraphAgent
from copilotkit.integrations.fastapi import add_fastapi_endpoint

from agent import wedding_graph

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

sdk = CopilotKitSDK(
    agents=[
        LangGraphAgent(
            name="Wedding_Planner",
            description="Helps you plan a personalized wedding using AG-UI protocol.",
            graph=wedding_graph
        )
    ]
)

add_fastapi_endpoint(app, sdk, "/copilotkit")

@app.get("/")
async def root():
    return {"message": "Wedding Planner Agent Running!"}

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(
        "langgraph_server:app",
        host="0.0.0.0",
        port=port,
        reload=True,
        timeout_keep_alive=300,
        timeout_graceful_shutdown=30,
        ws_ping_interval=20,
        ws_ping_timeout=20,
    )
