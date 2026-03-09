import { useQuery } from '@tanstack/react-query'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts'
import { fetchDashboardStats, fetchSegments } from '../api/client'
import { SkeletonCard } from '../components/Skeleton'
import ErrorState from '../components/ErrorState'

export default function Dashboard() {
  const stats = useQuery({ queryKey: ['dashboard-stats'], queryFn: fetchDashboardStats })
  const segments = useQuery({ queryKey: ['segments'], queryFn: fetchSegments })

  return (
    <div className="p-8 max-w-7xl mx-auto space-y-8">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Dashboard</h1>
          <p className="text-sm text-gray-500 mt-1">Klaflow pipeline overview</p>
        </div>
      </div>

      {/* KPI cards */}
      {stats.isLoading ? (
        <div className="grid grid-cols-4 gap-4">
          {Array.from({ length: 4 }).map((_, i) => <SkeletonCard key={i} />)}
        </div>
      ) : stats.isError ? (
        <ErrorState message={String(stats.error)} onRetry={() => stats.refetch()} />
      ) : (
        <div className="grid grid-cols-4 gap-4">
          <KpiCard label="Total Profiles" value={stats.data!.total_profiles.toLocaleString()} />
          <KpiCard label="Active Segments" value={String(stats.data!.active_segments)} />
          <KpiCard label="Events (24h)" value={stats.data!.events_24h.toLocaleString()} />
          <KpiCard label="Revenue (7d)" value={`$${stats.data!.revenue_7d.toLocaleString()}`} />
        </div>
      )}

      {/* Segment membership chart */}
      <div className="bg-white rounded-xl border border-gray-200 p-6">
        <h2 className="text-lg font-semibold text-gray-900 mb-4">Segment Membership</h2>
        {segments.isLoading ? (
          <div className="skeleton h-64 w-full" />
        ) : segments.isError ? (
          <ErrorState message={String(segments.error)} onRetry={() => segments.refetch()} />
        ) : (
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={segments.data!.map(s => ({
              name: s.segment_name.replace(/_/g, ' '),
              members: Number(s.member_count),
            }))}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis dataKey="name" tick={{ fontSize: 12 }} />
              <YAxis tick={{ fontSize: 12 }} />
              <Tooltip />
              <Bar dataKey="members" fill="#27AE60" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        )}
      </div>
    </div>
  )
}

function KpiCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="bg-white rounded-xl border border-gray-200 p-5">
      <p className="text-xs font-medium text-gray-500 uppercase tracking-wider">{label}</p>
      <p className="text-2xl font-bold text-gray-900 mt-1">{value}</p>
    </div>
  )
}
