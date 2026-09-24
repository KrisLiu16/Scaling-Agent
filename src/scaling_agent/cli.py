"""Command line: `scaling-agent coord serve | worker run | launch | status | ags check`."""

from __future__ import annotations

import asyncio
import json
import logging
import os

import typer

app = typer.Typer(no_args_is_help=True, help="Self-organized multi-agent harness.")
coord_app = typer.Typer(no_args_is_help=True, help="Coordination server.")
worker_app = typer.Typer(no_args_is_help=True, help="Worker runtime (runs inside a sandbox).")
ags_app = typer.Typer(no_args_is_help=True, help="Tencent Cloud AGS helpers.")
app.add_typer(coord_app, name="coord")
app.add_typer(worker_app, name="worker")
app.add_typer(ags_app, name="ags")


def _logging(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )


@coord_app.command("serve")
def coord_serve(verbose: bool = False) -> None:
    """Run the coordination server (settings from SA_COORD_* env vars)."""
    import uvicorn

    from .config import CoordSettings
    from .coord.server import build_app

    _logging(verbose)
    settings = CoordSettings()
    if not settings.admin_token:
        raise typer.BadParameter("set SA_COORD_ADMIN_TOKEN")
    uvicorn.run(build_app(settings), host=settings.host, port=settings.port, log_level="info")


@worker_app.command("run")
def worker_run() -> None:
    """Run one worker (settings from SA_WORKER_* env vars)."""
    from .runtime.main import main

    main()


@app.command()
def launch(run_file: str, verbose: bool = False) -> None:
    """Start a run described by a YAML file (see examples/run.example.yaml)."""
    from .config import RunConfig
    from .launcher import Launcher
    from .sandbox import build_provider
    from .workspace.gitea import GiteaClient

    _logging(verbose)
    run = RunConfig.load(run_file)
    admin = os.environ.get("SA_COORD_ADMIN_TOKEN")
    if not admin:
        raise typer.BadParameter("set SA_COORD_ADMIN_TOKEN")
    gitea_admin = None
    if os.environ.get("SA_GITEA_ADMIN_USER") and os.environ.get("SA_GITEA_ADMIN_PASSWORD"):
        # Basic auth: Gitea only lets Basic-authenticated admins mint tokens for worker accounts.
        gitea_admin = GiteaClient(
            run.gitea.url, username=os.environ["SA_GITEA_ADMIN_USER"], password=os.environ["SA_GITEA_ADMIN_PASSWORD"]
        )

    async def go() -> None:
        try:
            await Launcher(run, build_provider(run.provider, run.name), admin, gitea_admin).launch()
        finally:
            if gitea_admin:
                await gitea_admin.close()

    asyncio.run(go())


@app.command()
def status(coord_url: str = typer.Option("http://127.0.0.1:8700", envvar="SA_COORD_URL")) -> None:
    """Fleet KPIs from the coordination server."""
    import httpx

    token = os.environ.get("SA_COORD_ADMIN_TOKEN", "")
    resp = httpx.get(f"{coord_url.rstrip('/')}/api/admin/status", headers={"Authorization": f"Bearer {token}"}, timeout=30)
    resp.raise_for_status()
    typer.echo(json.dumps(resp.json(), indent=2, ensure_ascii=False))


@ags_app.command("check")
def ags_check(region: str = "ap-singapore", tool_name: str = "sa-worker") -> None:
    """Verify AK/SK and endpoint reachability; print quota and whether the worker tool exists."""
    from .sandbox.ags_control import AgsControlPlane

    cp = AgsControlPlane(os.environ["TENCENTCLOUD_SECRET_ID"], os.environ["TENCENTCLOUD_SECRET_KEY"], region)
    typer.echo(json.dumps({"quota(usage,limit)": cp.quota()}, indent=2))
    tool = cp.find_tool(tool_name)
    typer.echo(f"tool {tool_name}: " + (f"{tool.ToolId} status={tool.Status} persistent={tool.Persistent}" if tool else "absent"))


if __name__ == "__main__":
    app()
