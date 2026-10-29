# k.LAB NiFi: initial configuration and deployment

## Maintainer (existing working installation)

Preserve your working `nifi_conf` directory; **never replace it with template configuration**. 
In the repository root, edit `docker-compose.yml`, add `scripts/init-nifi-conf.sh`, and ensure `.gitignore` includes `nifi_conf/`, `_backup/`, `.env`, `raster/`, `excel/`, `parquet/` if these directories contain runtime data. The script is for a fresh clone only.

```powershell
cd C:\path\to\klab-nifi #change it with your directory to klab-nifi
docker compose config --quiet
git status
git add docker-compose.yml 
git add start_up_conf/init-nifi-conf.sh 
git add start_up_conf/DEPLOYMENT.md
git add .gitignore
git commit -m "Initialize NiFi conf before first Compose startup"
git push origin HEAD
```
## First installation

The shared `docker-compose.yml` expects a populated `nifi_conf` directory. Initialize it from the **same image version** before starting NiFi. This creates a local NiFi installation, not a copy of someone else's configuration.

```bash
cd ~/git/klab-nifi
git status --short                  # save any local edits first
git switch develop
git pull --ff-only origin develop
docker compose config --quiet
bash start_up_conf/init-nifi-conf.sh
test -f nifi_conf/nifi.properties && echo 'NiFi configuration is ready'
ls -l custom-nars/*.nar            # required by the current entrypoint
docker compose up -d --force-recreate nifi
docker compose ps
docker compose logs --tail=100 nifi
```

Open `https://localhost:8443/nifi`. Retrieve local single-user credentials from NiFi logs if needed. Import or configure the WEED process group and secrets separately. Do not copy another developer's certificates or credentials as a general deployment step.

## Existing working installation

**Do not** run the initialization script to replace an existing populated `nifi_conf`. Confirm it exists; back it up before deployment if the NiFi setup has important changes. Preserve queued FlowFiles and repository data before recreation.

```bash
cd ~/git/klab-nifi
test -f nifi_conf/nifi.properties || { echo 'Missing nifi.properties; stop and investigate'; exit 1; }
git status --short
git switch develop
git pull --ff-only origin develop
docker compose config --quiet
docker compose up -d --force-recreate nifi
docker compose logs --tail=100 nifi
```

## Safety / troubleshooting

- Never use `docker compose down -v` to repair this problem; it removes volume state.
- A bind-mounted empty `nifi_conf` hides the configuration bundled with the image.
- `nifi_conf` may include `flow.json.gz`, authentication, keystore and passwords; keep it out of Git.
- A fresh NiFi installation does **not** automatically contain the WEED `ListenHTTP -> EvaluateJsonPath -> HybridModellingStarter` process group. Export/import that flow separately.
- If the container stops at `cp /opt/nifi/custom-nars/*.nar`, build or copy the required custom NAR artifacts into `custom-nars/` first.
- If NiFi reports permission errors, inspect ownership on `nifi_conf` and image UID/GID.
