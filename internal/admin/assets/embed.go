package assets

import "embed"

//go:embed dashboard.css shadcn_templ.js htmx.min.js htmx-ext-sse.min.js
var FS embed.FS
