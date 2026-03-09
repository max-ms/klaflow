import { useParams, Link } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { fetchSegmentDetail, fetchSegmentCustomers } from '../api/client'
import { SkeletonTable } from '../components/Skeleton'
import ErrorState from '../components/ErrorState'

export default function SegmentDetail() {
  const { segmentName } = useParams<{ segmentName: string }>()

  const detail = useQuery({
    queryKey: ['segment-detail', segmentName],
    queryFn: () => fetchSegmentDetail(segmentName!),
    enabled: !!segmentName,
  })

  const customers = useQuery({
    queryKey: ['segment-customers', segmentName],
    queryFn: () => fetchSegmentCustomers(segmentName!, 100),
    enabled: !!segmentName,
  })

  return (
    <div className="p-8 max-w-7xl mx-auto space-y-6">
      <div className="text-sm text-gray-500">
        <Link to="/segments" className="hover:text-gray-700">Segments</Link>
        <span className="mx-2">/</span>
        <span className="text-gray-900 font-medium">{segmentName?.replace(/_/g, ' ')}</span>
      </div>

      {/* Header */}
      {detail.isLoading ? (
        <div className="skeleton h-24 w-full rounded-xl" />
      ) : detail.isError ? (
        <ErrorState message={String(detail.error)} onRetry={() => detail.refetch()} />
      ) : (
        <div className="bg-white rounded-xl border border-gray-200 p-6">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-bold text-gray-900 capitalize">
                {detail.data!.segment_name.replace(/_/g, ' ')}
              </h1>
              <p className="text-sm text-gray-500 mt-1 font-mono">{detail.data!.condition}</p>
            </div>
            <div className="text-right">
              <p className="text-3xl font-bold text-gray-900">
                {detail.data!.member_count.toLocaleString()}
              </p>
              <p className="text-sm text-gray-500">{detail.data!.percentage}% of all profiles</p>
            </div>
          </div>
        </div>
      )}

      {/* Members table */}
      <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
        <div className="px-6 py-4 border-b border-gray-200">
          <h2 className="text-lg font-semibold text-gray-900">Members</h2>
        </div>
        {customers.isLoading ? (
          <div className="p-6"><SkeletonTable rows={10} /></div>
        ) : customers.isError ? (
          <div className="p-6">
            <ErrorState message={String(customers.error)} onRetry={() => customers.refetch()} />
          </div>
        ) : (
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-gray-50 border-b border-gray-200">
                <th className="text-left px-4 py-3 font-semibold text-gray-600">Customer ID</th>
                <th className="text-left px-4 py-3 font-semibold text-gray-600">Account</th>
                <th className="text-left px-4 py-3 font-semibold text-gray-600">Evaluated At</th>
              </tr>
            </thead>
            <tbody>
              {customers.data!.map((c, i) => (
                <tr
                  key={`${c.account_id}-${c.customer_id}`}
                  className={`border-b border-gray-100 ${i % 2 === 1 ? 'bg-gray-50/50' : ''}`}
                >
                  <td className="px-4 py-3">
                    <Link
                      to={`/profiles/${c.customer_id}`}
                      className="text-klaviyo-green hover:text-klaviyo-green-dark font-mono text-xs"
                    >
                      {c.customer_id}
                    </Link>
                  </td>
                  <td className="px-4 py-3 text-xs text-gray-500">{c.account_id}</td>
                  <td className="px-4 py-3 text-xs text-gray-500">{c.evaluated_at}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}
