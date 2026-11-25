Bibliotecas de terceiros servidas junto com a página, para não depender de CDN:

- `maplibre-gl.js`, `maplibre-gl.css` — MapLibre GL JS 5.6.0 (BSD-3-Clause), copiados de `maplibre-gl/dist`.
- `parquet.js` — hyparquet 1.33.2 e hyparquet-compressors 1.1.3 (MIT), empacotados com esbuild:
  `esbuild entry.mjs --bundle --format=esm --minify`, em que `entry.mjs` reexporta `parquetReadObjects`,
  `parquetMetadataAsync` e `compressors`.
