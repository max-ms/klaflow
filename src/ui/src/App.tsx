import { Routes, Route, Navigate } from 'react-router-dom'
import Layout from './components/Layout'
import Dashboard from './pages/Dashboard'
import Profiles from './pages/Profiles'
import CustomerProfile from './pages/CustomerProfile'
import Segments from './pages/Segments'
import SegmentDetail from './pages/SegmentDetail'
import Decisions from './pages/Decisions'
import Flows from './pages/Flows'
import Campaigns from './pages/Campaigns'

export default function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route path="/" element={<Navigate to="/dashboard" replace />} />
        <Route path="/dashboard" element={<Dashboard />} />
        <Route path="/profiles" element={<Profiles />} />
        <Route path="/profiles/:customerId" element={<CustomerProfile />} />
        <Route path="/segments" element={<Segments />} />
        <Route path="/segments/:segmentName" element={<SegmentDetail />} />
        <Route path="/decisions" element={<Decisions />} />
        <Route path="/flows" element={<Flows />} />
        <Route path="/campaigns" element={<Campaigns />} />
      </Route>
    </Routes>
  )
}
