# %% [markdown]
# # Setup 01 — Seed dictionaries
# Part of the retail-demo setup utility. Re-runnable (overwrite-by-design).
#
# Fetches the dictionary JSON files into `Files/setup/dictionaries/` in the
# default lakehouse. Local-first: if the required files are already present,
# the download is skipped entirely. No engine code lives in this notebook.

# %%
# PARAMETERS — rendered by `retail-setup render`; defaults work unrendered
def _param(value: str, default: str) -> str:
    return default if len(value) > 1 and value[0] == value[1] == "{" else value

LAKEHOUSE_NAME = _param("{{LAKEHOUSE_NAME}}", "retail_lakehouse")
SILVER_DB = _param("{{SILVER_DB}}", "ag")
GOLD_DB = _param("{{GOLD_DB}}", "au")
STORE_TYPE = _param("{{STORE_TYPE}}", "supercenter")
START_DATE = _param("{{START_DATE}}", "2025-01-01")
END_DATE = _param("{{END_DATE}}", "2025-03-31")
STORE_COUNT = int(_param("{{STORE_COUNT}}", "50"))
SEED = int(_param("{{SEED}}", "42"))
DICTIONARY_REF = _param("{{DICTIONARY_REF}}", "main")

spark.conf.set("spark.sql.session.timeZone", "UTC")  # engine timestamps depend on it

# %% [markdown]
# ## Fetch dictionaries (local-first, pinned ref)

# %%
import os
import urllib.error
import urllib.request

# Local filesystem mount of the default lakehouse Files area.
LOCAL_ROOT = "/lakehouse/default/Files/setup/dictionaries"
# Same location as seen by mssparkutils (relative Files/ path).
FS_ROOT = "Files/setup/dictionaries"
BASE_URL = (
    "https://raw.githubusercontent.com/amattas/retail-demo/"
    f"{DICTIONARY_REF}/utility/data/dictionaries"
)

REQUIRED = [
    "_shared/first_names.json",
    "_shared/last_names.json",
    "_shared/geographies.json",
    "_shared/tax_rates.json",
    f"{STORE_TYPE}/profile.json",
    f"{STORE_TYPE}/products.json",
    f"{STORE_TYPE}/brands.json",
]
# tags.json is optional per store type — a 404 upstream is tolerated.
OPTIONAL = [f"{STORE_TYPE}/tags.json"]

if all(mssparkutils.fs.exists(f"{FS_ROOT}/{rel}") for rel in REQUIRED):
    print(f"All required dictionaries already present under {FS_ROOT} — skipping download.")
else:
    print(f"Fetching dictionaries from {BASE_URL}")
    for rel in REQUIRED + OPTIONAL:
        url = f"{BASE_URL}/{rel}"
        dest = f"{LOCAL_ROOT}/{rel}"
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        try:
            with urllib.request.urlopen(url) as resp:
                data = resp.read()
        except urllib.error.HTTPError as err:
            if err.code == 404 and rel in OPTIONAL:
                print(f"optional {rel}: not present upstream (404) — skipped")
                continue
            raise
        with open(dest, "wb") as fh:
            fh.write(data)
        print(f"fetched {rel} ({len(data):,} bytes)")

# %% [markdown]
# ## Manifest

# %%
manifest = []
for root, _dirs, files in os.walk(LOCAL_ROOT):
    for name in files:
        path = os.path.join(root, name)
        manifest.append((os.path.relpath(path, LOCAL_ROOT), os.path.getsize(path)))
for rel, size in sorted(manifest):
    print(f"{rel:50s} {size:>12,} bytes")
print(f"{len(manifest)} files under {LOCAL_ROOT}")
