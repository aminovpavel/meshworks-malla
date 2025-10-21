import { NavLink, Outlet } from "react-router-dom";
import "./App.css";

interface NavItem {
  to: string;
  label: string;
  end?: boolean;
}

const NAV_LINKS: NavItem[] = [
  { to: "/", label: "Дашборд", end: true },
  { to: "/map", label: "Карта" },
  { to: "/chat", label: "Чат" },
  { to: "/packets", label: "Пакеты" },
  { to: "/traceroutes", label: "Traceroute" },
  { to: "/longest-links", label: "Longest Links" },
];

function AppLayout() {
  return (
    <div className="app">
      <header className="app__header">
        <div className="app__brand">
          <span className="app__brand-accent" aria-hidden="true">
            ●
          </span>
          <span>Meshworks Malla</span>
        </div>
        <nav className="app__nav" aria-label="Страницы">
          {NAV_LINKS.map(({ to, label, end }) => (
            <NavLink
              key={to}
              to={to}
              end={Boolean(end)}
              className={({ isActive }) =>
                isActive ? "app__nav-link app__nav-link--active" : "app__nav-link"
              }
            >
              {label}
            </NavLink>
          ))}
        </nav>
      </header>
      <main className="app__content">
        <Outlet />
      </main>
    </div>
  );
}

export default AppLayout;
