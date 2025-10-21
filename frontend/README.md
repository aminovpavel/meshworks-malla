# Meshworks Malla SPA

This package contains the new single-page application that replaces the Flask
templates in `meshworks-malla`. The UI talks to meshpipe exclusively through
gRPC-Web (Envoy sidecar) and is bundled with Vite.

## Development

```bash
npm install
npm run proto:generate
npm run dev
```

The dev server expects the following environment variables:

- `VITE_GRPC_WEB_ENDPOINT` – gRPC-Web endpoint exposed by Envoy
  (defaults to `/grpc-web`).
- `VITE_MESHPIPE_TOKEN` – optional bearer token to include in request metadata.

You can create a local `.env` file next to `package.json` to override them.

## Regenerating protobuf stubs

We reuse the canonical proto files from `../src/malla/protos`. Updating the
proto in the Python package and running:

```bash
npm run proto:generate
```

will emit TypeScript definitions into `src/gen`. Those files are ignored by the
lint configuration but kept in the repo for reproducibility.

## Available scripts

| Script              | Purpose                                |
| ------------------- | -------------------------------------- |
| `npm run dev`       | Start Vite in development mode         |
| `npm run build`     | Type-check and produce the production bundle |
| `npm run lint`      | Run ESLint (generated code is ignored) |
| `npm run preview`   | Serve the production bundle locally    |
| `npm run proto:generate` | Regenerate gRPC-Web TypeScript stubs |

## Next steps

- replace the demo screen in `src/App.tsx` with real dashboard/chat views;
- integrate routing, query caching and design system;
- wire deployment so `Dockerfile` serves the static bundle via nginx while
  proxying gRPC-Web to Envoy.
