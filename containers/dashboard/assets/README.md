# assets

## `brazil-states.geojson`

Boundaries of the 27 Brazilian federative units (26 states + Distrito Federal) for the
choropleth on the "Mapa" page (docs/design/07-dashboard.md §4). Vendored: the dashboard
never downloads it at start.

- **Source:** Natural Earth, 1:50m Cultural Vectors, "Admin 1 – States, Provinces",
  release v5.1.2, file `geojson/ne_50m_admin_1_states_provinces.geojson` of
  <https://github.com/nvkelso/natural-earth-vector> (last change of that file: commit
  `117488dc884bad03366ff727eca013e434615127`, 2022-05-05; SHA-256 of the downloaded file
  `69a0e06e640b2d505858ae1cb63034e4677f3000b35a98e16312932b98c426b9`).
- **License:** public domain. "All versions of Natural Earth raster + vector map data
  found on this website are in the public domain. You may use the maps in any manner,
  including modifying the content and design" (<https://www.naturalearthdata.com/about/terms-of-use/>,
  `LICENSE.md` of the repository). No attribution required; credit: *Made with Natural Earth.*
- **Derivation (2026-10-24):** features with `adm0_a3 = "BRA"` (27), properties reduced
  to `state` (= `postal`, the two-letter code used by Olist's `customer_state`), `name`
  and `iso_3166_2`; feature `id` = the state code; coordinates rounded to 4 decimal
  places (about 11 m); 111 KB.

```python
import json

d = json.load(open("ne_50m_admin_1_states_provinces.geojson"))
r = lambda c: [r(x) for x in c] if isinstance(c[0], list) else [round(c[0], 4), round(c[1], 4)]
features = sorted(
    (
        {
            "type": "Feature",
            "id": f["properties"]["postal"],
            "properties": {
                "state": f["properties"]["postal"],
                "name": f["properties"]["name"],
                "iso_3166_2": f["properties"]["iso_3166_2"],
            },
            "geometry": {
                "type": f["geometry"]["type"],
                "coordinates": r(f["geometry"]["coordinates"]),
            },
        }
        for f in d["features"]
        if f["properties"]["adm0_a3"] == "BRA"
    ),
    key=lambda f: f["id"],
)
```
