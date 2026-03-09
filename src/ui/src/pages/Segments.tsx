import { useQuery } from '@tanstack/react-query'
import { Link } from 'react-router-dom'
import { fetchSegments, fetchDashboardStats } from '../api/client'
import { SkeletonTable } from '../components/Skeleton'
import ErrorState from '../components/ErrorState'

const conditionSummaries: Record<string, string> = {
  high_engagers: 'Open rate >= 30% or 2+ link clicks (7d)',
  recent_purchasers: '1+ purchases in last 7 days',
  at_risk: 'No purchase in 30d + inactive',
  active_browsers: '5+ page views in last 7 days',
  cart_abandoners: '1+ cart abandonment in 3 days',
  vip_lapsed: 'VIP tier + no purchase in 60 days',
  discount_sensitive: 'Discount sensitivity >= 60%',
}

export default function Segments() {
  const { data, isLoading, isError, error, refetch } = useQuery({
    queryKey: ['segments'],
    queryFn: fetchSegments,
  })
  const stats = useQuery({ queryKey: ['dashboard-stats'], queryFn: fetchDashboardStats })
  const totalProfiles = stats.data?.total_profiles || 1

  if (isLoading) {
    return (
      <div className="p-8 max-w-7xl mx-auto space-y-6">
        <h1 className="text-2xl font-bold text-gray-900">Segments</h1>
        <SkeletonTable rows={7} />
      </div>
    )
  }

  if (isError) {
    return (
      <div className="p-8 max-w-7xl mx-auto space-y-6">
        <h1 className="text-2xl font-bold text-gray-900">Segments</h1>
        <ErrorState message={String(error)} onRetry={refetch} />
      </div>
    )
  }

  return (
    <div className="p-8 max-w-7xl mx-auto space-y-6">
      <h1 className="text-2xl font-bold text-gray-900">Segments</h1>

      <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-gray-50 border-b border-gray-200">
              <th className="text-left px-4 py-3 font-semibold text-gray-600">Segment</th>
              <th className="text-left px-4 py-3 font-semibold text-gray-600">Members</th>
              <th className="text-left px-4 py-3 font-semibold text-gray-600">% of Total</th>
              <th className="text-left px-4 py-3 font-semibold text-gray-600">Condition</th>
            </tr>
          </thead>
          <tbody>
            {data!.map((s, i) => {
              const count = Number(s.member_count)
              const pct = ((count / totalProfiles) * 100).toFixed(1)
              return (
                <tr
                  key={s.segment_name}
                  className={`border-b border-gray-100 ${i % 2 === 1 ? 'bg-gray-50/50' : ''}`}
                >
                  <td className="px-4 py-3">
                    <Link
                      to={`/segments/${s.segment_name}`}
                      className="text-klaviyo-green hover:text-klaviyo-green-dark font-medium"
                    >
                      {s.segment_name.replace(/_/g, ' ')}
                    </Link>
                  </td>
                  <td className="px-4 py-3 font-mono">{count.toLocaleString()}</td>
                  <td className="px-4 py-3 text-gray-500">{pct}%</td>
                  <td className="px-4 py-3 text-xs text-gray-500">
                    {conditionSummaries[s.segment_name] || '-'}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}
