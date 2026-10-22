# Product artwork prototype

This isolated prototype composes small, deterministic retail product images from
an explicit SKU list and a set of editable SVG product-family templates. Each
output is a standalone 256 x 256 SVG; brand and product lettering is embedded as
SVG text, with no remote fonts, images, or runtime service.

Run from the repository root:

```powershell
python product-artwork\generate.py
```

The command writes `generated\manifest.json` and one SVG per distinct
brand/product/template combination. The manifest maps the input SKU to a stable
asset key and relative file path. Re-running with identical inputs produces
identical artwork and manifest contents.

`products.json` deliberately contains representative combinations from the
existing synthetic dictionaries: candy bar, carton, loose produce, tool,
handbag, and bottle. Template assignment is explicit; unknown templates fail
instead of receiving a misleading generic image. This is a visual and pipeline
prototype, not yet complete catalog coverage and not wired into retail data
generation, Power BI, or deployment.

The current SVGs are stylized vector mockups, not photo-real packshots, and
should not be treated as approved demo thumbnails. Production-quality artwork
needs licensed or generated neutral product-base images with brand and product
lettering composed over them; hand-drawn silhouettes are not an adequate
substitute. Review that image-based direction on a small contact sheet before
building out the catalog mapping.

Keep editable source templates in `templates\`. Do not hand-edit generated
files. After the visual direction and output consumers are agreed, the next
step is a catalog-wide audited mapping, followed by raster exports if a target
surface requires PNG. Publishing to GitHub Pages or another host is intentionally
not part of this prototype; the manifest and URL base should be separated before
adding remote asset fetching.
