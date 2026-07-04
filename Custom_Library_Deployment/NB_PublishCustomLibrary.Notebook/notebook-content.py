# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# ---- 0. Import all libraries ----------------------
import re
import requests
import notebookutils

# ---- 1. Setup the parameters ----------------------
WHL_NAME    = "dataguide_demolibrary-0.2.0-py3-none-any.whl"  # new version, sitting in notebook Resources
ENV_PATTERN = "_DE"                                # match environments named *_DE (suffix match)

# ---- 2. Setup -------------------------------------------------------
BASE  = "https://api.fabric.microsoft.com/v1"
TOKEN = notebookutils.credentials.getToken("pbi")  # token for the current user, no service principal needed
H     = {"Authorization": f"Bearer {TOKEN}"}

whl_path  = f"{notebookutils.nbResPath}/builtin/{WHL_NAME}"
whl_bytes = open(whl_path, "rb").read()
package   = re.split(r"[-_]\d", WHL_NAME)[0]       # "my_library" -> used to find & remove old versions
print(f"Wheel: {WHL_NAME}  ({len(whl_bytes)/1024:.0f} KB)  package='{package}'")

# ---- 3. Helpers -----------------------------------------------------
def get_all(url):
    """GET a Fabric list endpoint, following pagination."""
    items = []
    while url:
        r = requests.get(url, headers=H); r.raise_for_status()
        body = r.json()
        items += body.get("value", [])
        url = body.get("continuationUri")
    return items

def deploy(ws_id, env_id):
    libs = f"{BASE}/workspaces/{ws_id}/environments/{env_id}/staging/libraries"
    # 3a. remove any existing version of the same package
    existing = requests.get(f"{libs}?beta=false", headers=H).json().get("libraries", [])
    for lib in existing:
        name = lib.get("name", "")
        if (lib.get("libraryType") == "Custom" and name.endswith(".whl")
                and re.split(r"[-_]\d", name)[0] == package):
            requests.delete(f"{libs}/{name}", headers=H).raise_for_status()
    # 3b. upload the new wheel
    up = requests.post(f"{libs}/{WHL_NAME}",
                       headers={**H, "Content-Type": "application/octet-stream"},
                       data=whl_bytes)
    up.raise_for_status()
    # 3c. publish so attached notebooks pick up the change
    requests.post(f"{BASE}/workspaces/{ws_id}/environments/{env_id}/staging/publish",
                  headers=H).raise_for_status()

# ---- 4. Scan every workspace, deploy to each *_DE environment -------
updated = []
for ws in get_all(f"{BASE}/workspaces"):
    try:
        envs = get_all(f"{BASE}/workspaces/{ws['id']}/environments")
    except Exception:
        continue  # skip workspaces that don't expose environments
    for env in envs:
        if env["displayName"].endswith(ENV_PATTERN):
            try:
                deploy(ws["id"], env["id"])
                updated.append(ws["displayName"])
            except Exception as e:
                print(f"  ! Failed in {ws['displayName']} / {env['displayName']}: {e}")

# ---- 5. Report ------------------------------------------------------
print(f"\nUpdated {WHL_NAME} in {len(updated)} workspace(s):")
for name in sorted(updated):
    print(f"  - {name}")
print("\nPublish runs in the background (typically a few minutes per environment).")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
