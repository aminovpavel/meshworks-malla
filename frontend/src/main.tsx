import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { RouterProvider } from "react-router-dom";
import "./index.css";
import { MeshpipeLiveProvider } from "./lib/state/live";
import { router } from "./routes/router";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <MeshpipeLiveProvider>
      <RouterProvider router={router} />
    </MeshpipeLiveProvider>
  </StrictMode>,
);
