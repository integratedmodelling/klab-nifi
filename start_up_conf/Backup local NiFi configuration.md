# Back up the local NiFi configuration

This procedure creates a **timestamped, local backup** before changing Python processors, Docker Compose settings, or NiFi flow configuration. Run the commands in **PowerShell** from the `klab-nifi` repository root. They use relative paths, so each developer can use their own clone.

> **Scope:** This backs up NiFi's `conf` directory, `docker-compose.yml`, and the current `HybridModellingStarter.py`. It is **not a full NiFi disaster-recovery backup**: queued FlowFiles, content, provenance, component state, installed NAR files, and generated rasters are not included. Keep backups private: `conf` can include certificates, authentication files, and sensitive settings.

## 1. Prepare

**Task:** Start Docker Desktop (Windows) or the Docker engine (Linux/WSL) and ensure it is ready. In NiFi, stop incoming requests and check that no important work is in progress or queued.

Open PowerShell and navigate to **your own** clone:

```powershell
cd <path-to-your-klab-nifi-repository>
docker compose ps
```

Replace `<path-to-your-klab-nifi-repository>` with the actual path; do not type the angle brackets literally. Confirm that the service is named `nifi`. If the container is not running, you can still take a backup of the persistent `nifi_conf` directory, but copying from the container requires an existing container.

## 2. Create a timestamped destination

```powershell
$backup = Join-Path (Get-Location) ("_backup\nifi_" + (Get-Date -Format "yyyyMMdd_HHmmss"))
New-Item -ItemType Directory -Path $backup -Force | Out-Null
Write-Host "Backup directory: $backup"
Test-Path $backup
```

The final command should return `True`. Keep this PowerShell session open: subsequent commands use `$backup`.

## 3. Stop NiFi and copy its configuration (recommended)

**Task:** Confirm that no important FlowFiles are processing or queued. Stopping NiFi improves backup consistency, but does not back up its runtime repositories.

```powershell
docker compose stop nifi
docker compose cp nifi:/opt/nifi/nifi-current/conf "$backup\conf-stopped"
```

With the repository's current Compose setup, the configuration is also bind-mounted at `./nifi_conf`. If `docker compose cp` fails but that directory is populated, use the host copy as a fallback:

```powershell
# Fallback only; do not overwrite conf-stopped after a successful container copy.
Copy-Item .\nifi_conf "$backup\conf-stopped" -Recurse -Force
```

> Do **not** execute the fallback if `conf-stopped` already exists from the preceding `docker compose cp` operation: that would create an extra nested `nifi_conf` folder. On a new clone, never back up an empty `nifi_conf` directory as though it were a valid NiFi configuration.

## 4. Save deployment files and the Git revision

```powershell
Copy-Item .\docker-compose.yml "$backup\docker-compose.yml"
Copy-Item .\nifi-klab-nifi\src\main\python\processors\HybridModellingStarter.py "$backup\HybridModellingStarter.py"
git rev-parse HEAD | Set-Content "$backup\git_revision.txt"
```

If you modified other processors, copy those files too. The Git revision identifies the committed baseline, but **uncommitted modifications are not contained in the revision**; back up or commit those separately.

## 5. Verify the snapshot

```powershell
Test-Path "$backup\conf-stopped\flow.json.gz"
Test-Path "$backup\conf-stopped\nifi.properties"
Test-Path "$backup\docker-compose.yml"
Test-Path "$backup\HybridModellingStarter.py"
Test-Path "$backup\git_revision.txt"
Get-ChildItem "$backup\conf-stopped"
```

The five `Test-Path` results should all be `True`. If the flow file has a different configured name, confirm its expected path from `nifi.properties`. Resolve missing or empty files before relying on this backup.

You can additionally record checksums for later verification:

```powershell
Get-FileHash "$backup\conf-stopped\flow.json.gz", "$backup\HybridModellingStarter.py" -Algorithm SHA256
```

## 6. Resume NiFi when ready

If you only wanted a backup and are **not** redeploying code:

```powershell
docker compose start nifi
```

If you are deploying updated Python processor files using this project's Compose entrypoint, **after reviewing the changes**:

```powershell
docker compose up -d --force-recreate nifi
```

Then check startup and the NiFi UI:

```powershell
docker compose ps
docker compose logs --tail=100 nifi
```

**Task:** Open your NiFi URL (usually `https://localhost:8443/nifi`) and verify the flow and processor configuration.

## 7. Important safety and rollback notes

- Do **not** use `docker compose down -v` during backup or rollback; it can remove Docker volumes.
- This workflow saves one authoritative **stopped** configuration copy. The earlier two-copy method (`conf` while running, `conf-stopped` after stopping) is optional for comparison, not necessary for routine backups.
- To restore configuration later, **stop NiFi first**, preserve the current state separately, and restore the backed-up files to the `nifi_conf` bind-mount directory only if a configuration rollback is actually needed.
- To roll back a Python-only change, restore the saved processor or the prior Git revision, then recreate the NiFi container. Restoring `conf` alone does **not** roll back Python code.
- Ensure `_backup/`, `nifi_conf/`, and local secrets are excluded from Git. Never push these snapshots to the shared repository.
- For production-grade recovery, separately back up NiFi's content, FlowFile, provenance and state repositories, and record the container image, NAR versions, and flow dependencies.

## Quick command sequence (existing initialized installation)

```powershell
# Run from the repository root after verifying Docker and quiescing the flow.
docker compose ps
$backup = Join-Path (Get-Location) ("_backup\nifi_" + (Get-Date -Format "yyyyMMdd_HHmmss"))
New-Item -ItemType Directory -Path $backup -Force | Out-Null
docker compose stop nifi
docker compose cp nifi:/opt/nifi/nifi-current/conf "$backup\conf-stopped"
Copy-Item .\docker-compose.yml "$backup\docker-compose.yml"
Copy-Item .\nifi-klab-nifi\src\main\python\processors\HybridModellingStarter.py "$backup\HybridModellingStarter.py"
git rev-parse HEAD | Set-Content "$backup\git_revision.txt"
Test-Path "$backup\conf-stopped\flow.json.gz"
Test-Path "$backup\conf-stopped\nifi.properties"
# Continue with either: docker compose start nifi
# Or, for updated processor deployment: docker compose up -d --force-recreate nifi
```
