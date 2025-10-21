interface ImportMetaEnv {
  readonly VITE_GRPC_WEB_ENDPOINT?: string;
  readonly VITE_MESHPIPE_TOKEN?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
