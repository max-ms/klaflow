import { useQuery } from '@tanstack/react-query'
import { PieChart, Pie, Cell, ResponsiveContainer, Tooltip, Legend } from 'recharts'
import { fetchArmStats, fetchRecentDecisions } from '../api/client'
import { SkeletonTable, SkeletonCard } from '../components/Skeleton'
import ErrorState from '../components/ErrorState'

const ARM_COLORS = ['#27AE60', '#2980B9', '#E67E22', '#8E44AD', '#95A5A6']

export default function Decisions() {
  const armStats = useQuery({ queryKey: ['arm-stats'], queryFn: fetchArmStats })
  const recent = useQuery({
    queryKey: ['recent-decisions'],
    queryFn: () => fetchRecentDecisions(20),
    refetchInterval: 15_000,
  })

  const arms = armStats.data ? Object.entries(armStats.data) : []
  const totalAlpha = arms.reduce((sum, [, s]) => sum + s.alpha, 0)

  // Pie chart data
  const pieData = arms.map(([name, s]) => ({
    name: name.replace(/_/g, ' '),
    value: Math.round(s.expected_thompson * 100),
  }))

  return (
    <div className="p-8 max-w-7xl mx-auto space-y-8">
      <h1 className="text-2xl font-bold text-gray-900">Decisions</h1>
      <p className="text-sm text-gray-500">Contextual bandit operator view</p>

      <div className="grid grid-cols-2 gap-6">
        {/* Arm performance table */}
        <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
          <div className="px-6 py-4 border-b border-gray-200">
            <h2 className="text-lg font-semibold text-gray-900">Arm Performance</h2>
          </div>
          {armStats.isLoading ? (
            <div className="p-6"><SkeletonTable rows={5} /></div>
          ) : armStats.isError ? (
            <div className="p-6">
              <ErrorState message={String(armStats.error)} onRetry={() => armStats.refetch()} />
            </div>
          ) : (
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-gray-50 border-b border-gray-200">
                  <th className="text-left px-4 py-3 font-semibold text-gray-600">Arm</th>
                  <th className="text-left px-4 py-3 font-semibold text-gray-600">Alpha</th>
                  <th className="text-left px-4 py-3 font-semibold text-gray-600">Beta</th>
                  <th className="text-left px-4 py-3 font-semibold text-gray-600">Expected</th>
                  <th className="text-left px-4 py-3 font-semibold text-gray-600">Share</th>
                </tr>
              </thead>
              <tbody>
                {arms.map(([name, s], i) => (
                  <tr
                    key={name}
                    className={`border-b border-gray-100 ${i % 2 === 1 ? 'bg-gray-50/50' : ''}`}
                  >
                    <td className="px-4 py-3 font-mono text-xs">{name}</td>
                    <td className="px-4 py-3">{s.alpha.toFixed(1)}</td>
                    <td className="px-4 py-3">{s.beta.toFixed(1)}</td>
                    <td className="px-4 py-3 font-medium">
                      {(s.expected_thompson * 100).toFixed(1)}%
                    </td>
                    <td className="px-4 py-3">
                      <div className="w-16 h-2 bg-gray-200 rounded-full overflow-hidden">
                        <div
                          className="h-full bg-klaviyo-green rounded-full"
                          style={{ width: `${(s.alpha / Math.max(totalAlpha, 1)) * 100}%` }}
                        />
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>

        {/* Arm distribution pie chart */}
        <div className="bg-white rounded-xl border border-gray-200 p-6">
          <h2 className="text-lg font-semibold text-gray-900 mb-4">Arm Distribution</h2>
          {armStats.isLoading ? (
            <SkeletonCard />
          ) : armStats.isError ? (
            <ErrorState message={String(armStats.error)} onRetry={() => armStats.refetch()} />
          ) : (
            <ResponsiveContainer width="100%" height={280}>
              <PieChart>
                <Pie
                  data={pieData}
                  cx="50%"
                  cy="50%"
                  innerRadius={60}
                  outerRadius={100}
                  paddingAngle={2}
                  dataKey="value"
                >
                  {pieData.map((_, i) => (
                    <Cell key={i} fill={ARM_COLORS[i % ARM_COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip formatter={(value: number) => `${value}%`} />
                <Legend />
              </PieChart>
            </ResponsiveContainer>
          )}
        </div>
      </div>

      {/* Live decision feed */}
      <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
        <div className="px-6 py-4 border-b border-gray-200 flex items-center justify-between">
          <h2 className="text-lg font-semibold text-gray-900">Live Decision Feed</h2>
          <span className="text-xs text-gray-400">Auto-refresh every 15s</span>
        </div>
        {recent.isLoading ? (
          <div className="p-6"><SkeletonTable rows={8} /></div>
        ) : recent.isError ? (
          <div className="p-6">
            <ErrorState message={String(recent.error)} onRetry={() => recent.refetch()} />
          </div>
        ) : !recent.data?.length ? (
          <div className="p-12 text-center text-sm text-gray-400">
            No decisions recorded yet. Use the "Get Decision" button on a customer profile.
          </div>
        ) : (
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-gray-50 border-b border-gray-200">
                <th className="text-left px-4 py-3 font-semibold text-gray-600">Timestamp</th>
                <th className="text-left px-4 py-3 font-semibold text-gray-600">Customer</th>
                <th className="text-left px-4 py-3 font-semibold text-gray-600">Account</th>
                <th className="text-left px-4 py-3 font-semibold text-gray-600">Arm Chosen</th>
              </tr>
            </thead>
            <tbody>
              {recent.data.map((d, i) => (
                <tr
                  key={i}
                  className={`border-b border-gray-100 ${i % 2 === 1 ? 'bg-gray-50/50' : ''}`}
                >
                  <td className="px-4 py-3 text-xs text-gray-500">{d.created_at}</td>
                  <td className="px-4 py-3 font-mono text-xs">{d.customer_id}</td>
                  <td className="px-4 py-3 text-xs text-gray-500">{d.account_id}</td>
                  <td className="px-4 py-3">
                    <span className="inline-block px-2 py-0.5 rounded text-xs font-medium bg-navy-900/10 text-navy-900">
                      {d.arm_chosen}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}
