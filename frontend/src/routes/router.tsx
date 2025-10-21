import { createBrowserRouter, Navigate } from "react-router-dom";
import AppLayout from "../App";
import { DashboardPage } from "../features/dashboard/DashboardPage";
import { MapPage } from "../features/map/MapPage";
import { ChatPage } from "../features/chat/ChatPage";
import { PacketsPage } from "../features/packets/PacketsPage";
import { TraceroutesPage } from "../features/traceroutes/TraceroutesPage";
import { LongestLinksPage } from "../features/longest-links/LongestLinksPage";

export const router = createBrowserRouter(
  [
    {
      path: "/",
      element: <AppLayout />,
      children: [
        { index: true, element: <DashboardPage /> },
        { path: "map", element: <MapPage /> },
        { path: "chat", element: <ChatPage /> },
        { path: "packets", element: <PacketsPage /> },
        { path: "traceroutes", element: <TraceroutesPage /> },
        { path: "longest-links", element: <LongestLinksPage /> },
        { path: "*", element: <Navigate to="/" replace /> },
      ],
    },
  ],
  {
    basename: import.meta.env.BASE_URL ?? "/",
  },
);
