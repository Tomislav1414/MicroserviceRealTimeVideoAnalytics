import { BrowserRouter, Routes, Route } from "react-router-dom";
import { NotificationsProvider } from "./context/NotificationsContext";
import { OccupancyProvider } from "./context/OccupancyContext";
import Layout from "./components/Layout";
import LivePage from "./pages/LivePage";
import HistoryPage from "./pages/HistoryPage";

export default function App() {
  return (
    // NotificationsProvider sits above the router (and thus above every
    // route) so its EventSource connection and accumulated state survive
    // navigating between pages — that persistence is what makes a
    // notification visible even when you've navigated away from the
    // camera it's about.
    <NotificationsProvider>
      <OccupancyProvider>
        <BrowserRouter>
          <Routes>
            <Route element={<Layout />}>
              <Route index element={<LivePage />} />
              <Route path="history" element={<HistoryPage />} />
            </Route>
          </Routes>
        </BrowserRouter>
      </OccupancyProvider>
    </NotificationsProvider>
  );
}
