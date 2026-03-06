import uvicorn
from app import app, state


def main() -> None:
    config = uvicorn.Config(app, host="0.0.0.0", port=8090, log_level="info")
    server = uvicorn.Server(config)
    state.stop_server = lambda: setattr(server, "should_exit", True)
    server.run()


if __name__ == "__main__":
    main()
