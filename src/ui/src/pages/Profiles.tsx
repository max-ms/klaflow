import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useNavigate } from 'react-router-dom'
import { fetchCustomers, fetchCustomerSegments } from '../api/client'
import { LifecycleBadge, ClvBadge, SegmentPill } from '../components/Badge'
import { SkeletonTable } from '../components/Skeleton'
import ErrorState from '../components/ErrorState'

export default function Profiles() {
  const [page, setPage] = useState(1)
  const [search, setSearch] = useState('')
  const [searchInput, setSearchInput] = useState('')
  const navigate = useNavigate()

  const { data, isLoading, isError, error, refetch } = useQuery({
    queryKey: ['customers', page, search],
    queryFn: () => fetchCustomers(page, search),
  })

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault()
    setSearch(searchInput)
    setPage(1)
  }

  const totalPages = data ? Math.ceil(data.total / data.page_size) : 0

  return (
    <div className="p-8 max-w-7xl mx-auto space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold text-gray-900">Profiles</h1>
        <form onSubmit={handleSearch} className="flex gap-2">
          <input
            type="text"
            placeholder="Search customer ID..."
            value={searchInput}
            onChange={(e) => setSearchInput(e.target.value)}
            className="border border-gray-300 rounded-lg px-3 py-2 text-sm w-64 focus:ring-2 focus:ring-klaviyo-green/30 focus:border-klaviyo-green outline-none"
          />
          <button
            type="submit"
            className="bg-klaviyo-green text-white px-4 py-2 rounded-lg text-sm font-medium hover:bg-klaviyo-green-dark transition-colors"
          >
            Search
          </button>
        </form>
      </div>

      {isLoading ? (
        <SkeletonTable rows={10} />
      ) : isError ? (
        <ErrorState message={String(error)} onRetry={refetch} />
      ) : !data?.customers.length ? (
        <EmptyState />
      ) : (
        <>
          <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-gray-50 border-b border-gray-200">
                  <th className="text-left px-4 py-3 font-semibold text-gray-600 sticky top-0 bg-gray-50">Customer ID</th>
                  <th className="text-left px-4 py-3 font-semibold text-gray-600">Lifecycle</th>
                  <th className="text-left px-4 py-3 font-semibold text-gray-600">CLV Tier</th>
                  <th className="text-left px-4 py-3 font-semibold text-gray-600">Churn Risk</th>
                  <th className="text-left px-4 py-3 font-semibold text-gray-600">Segments</th>
                  <th className="text-left px-4 py-3 font-semibold text-gray-600">Merchant</th>
                </tr>
              </thead>
              <tbody>
                {data.customers.map((c, i) => (
                  <tr
                    key={c.customer_id}
                    onClick={() => navigate(`/profiles/${c.customer_id}`)}
                    className={`border-b border-gray-100 cursor-pointer transition-colors hover:bg-gray-50 ${
                      i % 2 === 1 ? 'bg-gray-50/50' : ''
                    }`}
                  >
                    <td className="px-4 py-3 font-mono text-xs text-gray-900">{c.customer_id}</td>
                    <td className="px-4 py-3"><LifecycleBadge state={c.lifecycle_state} /></td>
                    <td className="px-4 py-3"><ClvBadge tier={c.clv_tier} /></td>
                    <td className="px-4 py-3">
                      <ChurnBar value={c.churn_risk_score} />
                    </td>
                    <td className="px-4 py-3">
                      <CustomerSegments customerId={c.customer_id} />
                    </td>
                    <td className="px-4 py-3 text-xs text-gray-500">{c.account_id}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {/* Pagination */}
          <div className="flex items-center justify-between text-sm text-gray-600">
            <span>
              Showing {(page - 1) * 50 + 1}–{Math.min(page * 50, data.total)} of{' '}
              {data.total.toLocaleString()} profiles
            </span>
            <div className="flex gap-2">
              <button
                onClick={() => setPage((p) => Math.max(1, p - 1))}
                disabled={page === 1}
                className="px-3 py-1.5 rounded-lg border border-gray-300 disabled:opacity-40 hover:bg-gray-50"
              >
                Previous
              </button>
              <button
                onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
                disabled={page >= totalPages}
                className="px-3 py-1.5 rounded-lg border border-gray-300 disabled:opacity-40 hover:bg-gray-50"
              >
                Next
              </button>
            </div>
          </div>
        </>
      )}
    </div>
  )
}

function ChurnBar({ value }: { value: number }) {
  const pct = Math.round(value * 100)
  const color = value < 0.3 ? 'bg-green-500' : value < 0.6 ? 'bg-yellow-500' : 'bg-red-500'
  return (
    <div className="flex items-center gap-2">
      <div className="w-16 h-2 bg-gray-200 rounded-full overflow-hidden">
        <div className={`h-full rounded-full ${color}`} style={{ width: `${pct}%` }} />
      </div>
      <span className="text-xs text-gray-500">{pct}%</span>
    </div>
  )
}

function CustomerSegments({ customerId }: { customerId: string }) {
  const { data } = useQuery({
    queryKey: ['customer-segments', customerId],
    queryFn: () => fetchCustomerSegments(customerId),
    staleTime: 60_000,
  })

  if (!data) return <span className="text-xs text-gray-400">...</span>
  if (!data.length) return <span className="text-xs text-gray-400">none</span>

  const show = data.slice(0, 3)
  const more = data.length - 3

  return (
    <div className="flex flex-wrap gap-1">
      {show.map((s) => (
        <SegmentPill key={s.segment_name} name={s.segment_name} />
      ))}
      {more > 0 && <span className="text-xs text-gray-500">+{more}</span>}
    </div>
  )
}

function EmptyState() {
  return (
    <div className="bg-white rounded-xl border border-gray-200 p-12 text-center">
      <p className="text-gray-400 text-sm">No profiles found. Run the data pipeline to populate customer data.</p>
    </div>
  )
}
