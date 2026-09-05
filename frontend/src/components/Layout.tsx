import { NavLink, Outlet } from "react-router-dom";
import NotificationBell from "./NotificationBell";
import NotificationToastStack from "./NotificationToast";

export default function Layout() {
  return (
    <div className="app-shell">
      <header className="app-header">
        <h1>VMS Operator Console</h1>
        <nav>
          <NavLink to="/" end>
            Layout 1
          </NavLink>
          <NavLink to="/layout2">Layout 2</NavLink>
          <NavLink to="/history">History</NavLink>
        </nav>
        <NotificationBell />
      </header>
      <main>
        <Outlet />
      </main>
      {/* Mounted alongside the routed page (not inside it) so a toast for
          any camera can appear no matter which page/route is active. */}
      <NotificationToastStack />
    </div>
  );
}
