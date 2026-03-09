import { useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import { useQuery, useMutation } from '@tanstack/react-query'
import {
  fetchCustomerFeatures,
  fetchCustomerScores,
  fetchCustomerAllSegments,
  fetchCustomerEvents,
  fetchDecisionHistory,
  postDecide,
} from '../api/client'
import { LifecycleBadge, ClvBadge, SegmentPill } from '../components/Badge'
import { SkeletonTable } from '../components/Skeleton'
import ErrorState from '../components/ErrorState'

export default function CustomerProfile() {
  const { customerId } = useParams<{ customerId: string }>()
  const [activeTab, setActiveTab] = useState<'activity' | 'segments' | 'decisions' | 'features'>('activity')

  const features = useQuery({
    queryKey: ['customer-features', customerId],
    queryFn: () => fetchCustomerFeatures(customerId!),
    enabled: !!customerId,
  })
  const scores = useQuery({
    queryKey: ['customer-scores', customerId],
    queryFn: () => fetchCustomerScores(customerId!),
    enabled: !!customerId,
  })
  const allSegments = useQuery({
    queryKey: ['customer-all-segments', customerId],
    queryFn: () => fetchCustomerAllSegments(customerId!),
    enabled: !!customerId,
  })

  const accountId = features.data?.account_id || ''
  const lifecycle = features.data?.lifecycle_state || 'unknown'
  const inSegments = allSegments.data?.filter((s) => Number(s.in_segment) === 1) || []
  const outSegments = allSegments.data?.filter((s) => Number(s.in_segment) === 0) || []

  const decide = useMutation({
    mutationFn: () => postDecide(customerId!, accountId),
  })

  const churnRisk = scores.data?.churn_risk_score ?? 0
  const churnColor = churnRisk < 0.3 ? 'text-green-600' : churnRisk < 0.6 ? 'text-yellow-600' : 'text-red-600'

  return (
    <div className="p-8 max-w-7xl mx-auto space-y-6">
      {/* Breadcrumb */}
      <div className="text-sm text-gray-500">
        <Link to="/profiles" className="hover:text-gray-700">Profiles</Link>
        <span className="mx-2">/</span>
        <span className="text-gray-900 font-medium">{customerId}</span>
      </div>

      <div className="flex gap-6">
        {/* Left panel */}
        <div className="w-1/3 space-y-4">
          <div className="bg-white rounded-xl border border-gray-200 p-6 space-y-4">
            <div>
              <h2 className="text-lg font-bold text-gray-900 font-mono">{customerId}</h2>
              {accountId && (
                <span className="inline-block mt-1 px-2 py-0.5 rounded text-xs bg-gray-100 text-gray-600">
                  {accountId}
                </span>
              )}
            </div>
            <div>
              <LifecycleBadge state={lifecycle} />
            </div>

            {/* Property grid */}
            <div className="space-y-3 text-sm">
              <PropRow label="CLV Tier">
                {scores.data ? <ClvBadge tier={scores.data.clv_tier} /> : <span className="skeleton w-12 h-4 inline-block" />}
              </PropRow>
              <PropRow label="Churn Risk">
                {scores.data ? (
                  <span className={`font-semibold ${churnColor}`}>
                    {Math.round(churnRisk * 100)}%
                  </span>
                ) : <span className="skeleton w-10 h-4 inline-block" />}
              </PropRow>
              <PropRow label="Discount Sensitivity">
                {scores.data ? (
                  <span>{Math.round(scores.data.discount_sensitivity * 100)}%</span>
                ) : <span className="skeleton w-10 h-4 inline-block" />}
              </PropRow>
              <PropRow label="Preferred Send Hour">
                {features.data?.preferred_send_hour !== undefined ? (
                  <span>{features.data.preferred_send_hour}:00</span>
                ) : <span className="skeleton w-10 h-4 inline-block" />}
              </PropRow>
            </div>

            {/* Segments */}
            <div>
              <p className="text-xs font-medium text-gray-500 uppercase mb-2">Segments</p>
              {allSegments.isLoading ? (
                <div className="skeleton h-6 w-full" />
              ) : inSegments.length ? (
                <div className="flex flex-wrap gap-1.5">
                  {inSegments.map((s) => (
                    <SegmentPill key={s.segment_name} name={s.segment_name} />
                  ))}
                </div>
              ) : (
                <p className="text-xs text-gray-400">No segments</p>
              )}
            </div>

            {/* Get Decision button */}
            <button
              onClick={() => decide.mutate()}
              disabled={decide.isPending || !accountId}
              className="w-full bg-klaviyo-green text-white py-2.5 rounded-lg text-sm font-medium hover:bg-klaviyo-green-dark transition-colors disabled:opacity-50"
            >
              {decide.isPending ? 'Deciding...' : 'Get Decision'}
            </button>
            {decide.data && (
              <div className="bg-green-50 border border-green-200 rounded-lg p-3 text-sm space-y-1">
                <p className="font-medium text-green-800">
                  Arm: <span className="font-mono">{decide.data.arm}</span>
                </p>
                <p className="text-green-700">Score: {decide.data.score}</p>
              </div>
            )}
            {decide.isError && (
              <ErrorState message={String(decide.error)} />
            )}
          </div>
        </div>

        {/* Right panel */}
        <div className="w-2/3">
          <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
            {/* Tabs */}
            <div className="border-b border-gray-200 flex">
              {(['activity', 'segments', 'decisions', 'features'] as const).map((tab) => (
                <button
                  key={tab}
                  onClick={() => setActiveTab(tab)}
                  className={`px-5 py-3 text-sm font-medium capitalize transition-colors ${
                    activeTab === tab
                      ? 'text-klaviyo-green border-b-2 border-klaviyo-green'
                      : 'text-gray-500 hover:text-gray-700'
                  }`}
                >
                  {tab}
                </button>
              ))}
            </div>

            <div className="p-5">
              {activeTab === 'activity' && <ActivityTab customerId={customerId!} />}
              {activeTab === 'segments' && <SegmentsTab inSegments={inSegments} outSegments={outSegments} />}
              {activeTab === 'decisions' && <DecisionsTab customerId={customerId!} />}
              {activeTab === 'features' && <FeaturesTab customerId={customerId!} />}
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}

function PropRow({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex justify-between items-center">
      <span className="text-gray-500">{label}</span>
      <span className="text-gray-900">{children}</span>
    </div>
  )
}

function ActivityTab({ customerId }: { customerId: string }) {
  const [page, setPage] = useState(1)
  const { data, isLoading, isError, error, refetch } = useQuery({
    queryKey: ['customer-events', customerId, page],
    queryFn: () => fetchCustomerEvents(customerId, page),
  })

  if (isLoading) return <SkeletonTable rows={8} />
  if (isError) return <ErrorState message={String(error)} onRetry={refetch} />
  if (!data?.events.length) return <p className="text-sm text-gray-400 py-4">No activity recorded.</p>

  return (
    <div className="space-y-3">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-gray-200">
            <th className="text-left py-2 font-semibold text-gray-600">Metric</th>
            <th className="text-left py-2 font-semibold text-gray-600">Value</th>
            <th className="text-left py-2 font-semibold text-gray-600">Computed At</th>
          </tr>
        </thead>
        <tbody>
          {data.events.map((e, i) => (
            <tr key={i} className={`border-b border-gray-50 ${i % 2 === 1 ? 'bg-gray-50/50' : ''}`}>
              <td className="py-2 font-mono text-xs">{e.metric_name}</td>
              <td className="py-2">{Number(e.metric_value).toFixed(2)}</td>
              <td className="py-2 text-gray-500 text-xs">{e.computed_at}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <div className="flex gap-2 justify-end">
        <button
          onClick={() => setPage((p) => Math.max(1, p - 1))}
          disabled={page === 1}
          className="px-3 py-1 text-xs border rounded disabled:opacity-40"
        >
          Prev
        </button>
        <span className="text-xs text-gray-500 py-1">Page {page}</span>
        <button
          onClick={() => setPage((p) => p + 1)}
          disabled={data.events.length < data.page_size}
          className="px-3 py-1 text-xs border rounded disabled:opacity-40"
        >
          Next
        </button>
      </div>
    </div>
  )
}

function SegmentsTab({
  inSegments,
  outSegments,
}: {
  inSegments: { segment_name: string; evaluated_at: string }[]
  outSegments: { segment_name: string; evaluated_at: string }[]
}) {
  return (
    <div className="space-y-6">
      <div>
        <h3 className="text-sm font-semibold text-gray-700 mb-2">Member of</h3>
        {inSegments.length ? (
          <div className="space-y-2">
            {inSegments.map((s) => (
              <div key={s.segment_name} className="flex items-center justify-between py-1.5 px-3 bg-green-50 rounded-lg">
                <SegmentPill name={s.segment_name} />
                <span className="text-xs text-gray-500">{s.evaluated_at}</span>
              </div>
            ))}
          </div>
        ) : (
          <p className="text-xs text-gray-400">Not in any segments</p>
        )}
      </div>
      <div>
        <h3 className="text-sm font-semibold text-gray-400 mb-2">Not in</h3>
        {outSegments.length ? (
          <div className="space-y-2">
            {outSegments.map((s) => (
              <div key={s.segment_name} className="flex items-center justify-between py-1.5 px-3 bg-gray-50 rounded-lg opacity-60">
                <span className="text-xs text-gray-500">{s.segment_name.replace(/_/g, ' ')}</span>
                <span className="text-xs text-gray-400">{s.evaluated_at}</span>
              </div>
            ))}
          </div>
        ) : null}
      </div>
    </div>
  )
}

function DecisionsTab({ customerId }: { customerId: string }) {
  const { data, isLoading, isError, error, refetch } = useQuery({
    queryKey: ['decision-history', customerId],
    queryFn: () => fetchDecisionHistory(customerId),
  })

  if (isLoading) return <SkeletonTable rows={5} />
  if (isError) return <ErrorState message={String(error)} onRetry={refetch} />
  if (!data?.length) return <p className="text-sm text-gray-400 py-4">No decisions recorded.</p>

  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="border-b border-gray-200">
          <th className="text-left py-2 font-semibold text-gray-600">Timestamp</th>
          <th className="text-left py-2 font-semibold text-gray-600">Arm</th>
          <th className="text-left py-2 font-semibold text-gray-600">Reward</th>
        </tr>
      </thead>
      <tbody>
        {data.map((d, i) => (
          <tr key={i} className={`border-b border-gray-50 ${i % 2 === 1 ? 'bg-gray-50/50' : ''}`}>
            <td className="py-2 text-xs text-gray-500">{d.created_at}</td>
            <td className="py-2 font-mono text-xs">{d.arm_chosen}</td>
            <td className="py-2">
              {d.reward ? (
                <span className={Number(d.reward) > 0 ? 'text-green-600' : 'text-red-600'}>
                  {Number(d.reward).toFixed(2)}
                </span>
              ) : (
                <span className="text-gray-400">-</span>
              )}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

function FeaturesTab({ customerId }: { customerId: string }) {
  const { data, isLoading, isError, error, refetch } = useQuery({
    queryKey: ['customer-features', customerId],
    queryFn: () => fetchCustomerFeatures(customerId),
  })

  const labels: Record<string, string> = {
    email_open_rate_7d: 'Email Open Rate (7d)',
    email_open_rate_30d: 'Email Open Rate (30d)',
    purchase_count_30d: 'Purchase Count (30d)',
    days_since_last_purchase: 'Days Since Last Purchase',
    avg_order_value: 'Avg Order Value',
    cart_abandon_rate_30d: 'Cart Abandon Rate (30d)',
    lifecycle_state: 'Lifecycle State',
    clv_tier: 'CLV Tier',
    churn_risk_score: 'Churn Risk Score',
    discount_sensitivity: 'Discount Sensitivity',
    preferred_send_hour: 'Preferred Send Hour',
    updated_at: 'Last Updated',
    account_id: 'Account ID',
  }

  if (isLoading) return <SkeletonTable rows={10} />
  if (isError) return <ErrorState message={String(error)} onRetry={refetch} />
  if (!data) return <p className="text-sm text-gray-400 py-4">No features found.</p>

  return (
    <table className="w-full text-sm">
      <thead>
        <tr className="border-b border-gray-200">
          <th className="text-left py-2 font-semibold text-gray-600">Property</th>
          <th className="text-left py-2 font-semibold text-gray-600">Value</th>
        </tr>
      </thead>
      <tbody>
        {Object.entries(data).map(([key, val], i) => (
          <tr key={key} className={`border-b border-gray-50 ${i % 2 === 1 ? 'bg-gray-50/50' : ''}`}>
            <td className="py-2 text-gray-600">{labels[key] || key}</td>
            <td className="py-2 font-mono text-xs">{val}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}
