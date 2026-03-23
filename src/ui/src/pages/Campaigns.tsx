import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { fetchCampaigns, createCampaign, CampaignSummary } from '../api/client'
import { SkeletonTable } from '../components/Skeleton'
import ErrorState from '../components/ErrorState'

const ARM_COLORS: Record<string, string> = {
  email_no_offer: '#27AE60',
  email_10pct_discount: '#2980B9',
  email_free_shipping: '#E67E22',
  sms_nudge: '#8E44AD',
  no_send: '#95A5A6',
}

const STATUS_COLORS: Record<string, string> = {
  completed: 'bg-green-100 text-green-800',
  executing: 'bg-blue-100 text-blue-800',
  planning: 'bg-amber-100 text-amber-800',
  failed: 'bg-red-100 text-red-800',
  paused: 'bg-gray-100 text-gray-600',
  cancelled: 'bg-gray-100 text-gray-600',
}

export default function Campaigns() {
  const queryClient = useQueryClient()
  const [showForm, setShowForm] = useState(false)
  const [accountId, setAccountId] = useState('merchant_001')
  const [goal, setGoal] = useState('')
  const [expanded, setExpanded] = useState<string | null>(null)

  const campaigns = useQuery({
    queryKey: ['campaigns'],
    queryFn: () => fetchCampaigns(),
    refetchInterval: 10_000,
  })

  const create = useMutation({
    mutationFn: () => createCampaign(accountId, goal),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['campaigns'] })
      setGoal('')
      setShowForm(false)
    },
  })

  return (
    <div className="p-8 max-w-7xl mx-auto space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Campaigns</h1>
          <p className="text-sm text-gray-500 mt-1">AI agent campaign management</p>
        </div>
        <button
          onClick={() => setShowForm(!showForm)}
          className="px-4 py-2 bg-navy-900 text-white text-sm font-medium rounded-lg hover:bg-navy-800 transition-colors"
        >
          {showForm ? 'Cancel' : 'New Campaign'}
        </button>
      </div>

      {/* Campaign creation form */}
      {showForm && (
        <div className="bg-white rounded-xl border border-gray-200 p-6 space-y-4">
          <h2 className="text-lg font-semibold text-gray-900">Create Campaign</h2>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Account ID</label>
            <select
              value={accountId}
              onChange={(e) => setAccountId(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-navy-900/20"
            >
              {['merchant_001', 'merchant_002', 'merchant_003', 'merchant_004', 'merchant_005'].map(
                (m) => (
                  <option key={m} value={m}>{m}</option>
                ),
              )}
            </select>
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Campaign Goal</label>
            <textarea
              value={goal}
              onChange={(e) => setGoal(e.target.value)}
              rows={3}
              placeholder="e.g. Send a win-back offer to customers who haven't purchased in 45 days. Skip anyone contacted in the last 14 days."
              className="w-full px-3 py-2 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-navy-900/20 resize-none"
            />
          </div>
          <button
            onClick={() => create.mutate()}
            disabled={!goal.trim() || create.isPending}
            className="px-4 py-2 bg-klaviyo-green text-white text-sm font-medium rounded-lg hover:bg-klaviyo-green/90 transition-colors disabled:opacity-50"
          >
            {create.isPending ? 'Executing...' : 'Launch Campaign'}
          </button>
          {create.isError && (
            <p className="text-sm text-red-600">{String(create.error)}</p>
          )}
        </div>
      )}

      {/* Campaign list */}
      {campaigns.isLoading ? (
        <div className="bg-white rounded-xl border border-gray-200 p-6">
          <SkeletonTable rows={5} />
        </div>
      ) : campaigns.isError ? (
        <ErrorState message={String(campaigns.error)} onRetry={() => campaigns.refetch()} />
      ) : !campaigns.data?.length ? (
        <div className="bg-white rounded-xl border border-gray-200 p-12 text-center">
          <p className="text-gray-400 text-sm">
            No campaigns yet. Click "New Campaign" to create one using natural language.
          </p>
        </div>
      ) : (
        <div className="space-y-4">
          {campaigns.data.map((c) => (
            <CampaignCard
              key={c.campaign_id}
              campaign={c}
              isExpanded={expanded === c.campaign_id}
              onToggle={() => setExpanded(expanded === c.campaign_id ? null : c.campaign_id)}
            />
          ))}
        </div>
      )}
    </div>
  )
}

function CampaignCard({
  campaign: c,
  isExpanded,
  onToggle,
}: {
  campaign: CampaignSummary
  isExpanded: boolean
  onToggle: () => void
}) {
  const totalScheduled = c.scheduled_count || 0

  return (
    <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
      {/* Header */}
      <button
        onClick={onToggle}
        className="w-full px-6 py-4 flex items-center justify-between hover:bg-gray-50/50 transition-colors text-left"
      >
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-3">
            <span className="font-mono text-xs text-gray-400">{c.campaign_id}</span>
            <span className={`inline-block px-2 py-0.5 rounded-full text-xs font-medium ${STATUS_COLORS[c.status] || 'bg-gray-100 text-gray-600'}`}>{c.status}</span>
            <span className="text-xs text-gray-400">{c.account_id}</span>
          </div>
          <p className="text-sm text-gray-900 mt-1 truncate">{c.goal}</p>
        </div>
        <div className="flex items-center gap-6 ml-4 shrink-0">
          <Stat label="Targeted" value={c.targeted_count} />
          <Stat label="Scheduled" value={totalScheduled} />
          <Stat label="Excluded" value={c.excluded_count} />
          <Stat label="Tool Calls" value={c.tool_call_count} />
          <svg
            className={`w-4 h-4 text-gray-400 transition-transform ${isExpanded ? 'rotate-180' : ''}`}
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
            strokeWidth={2}
          >
            <path strokeLinecap="round" strokeLinejoin="round" d="M19 9l-7 7-7-7" />
          </svg>
        </div>
      </button>

      {/* Expanded detail */}
      {isExpanded && (
        <div className="border-t border-gray-200 px-6 py-5 space-y-5">
          {/* Arm breakdown */}
          {Object.keys(c.arm_breakdown).length > 0 && (
            <div>
              <h3 className="text-sm font-semibold text-gray-700 mb-2">Arm Breakdown</h3>
              <div className="flex gap-2 flex-wrap">
                {Object.entries(c.arm_breakdown)
                  .sort(([, a], [, b]) => b - a)
                  .map(([arm, count]) => (
                    <div
                      key={arm}
                      className="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-gray-50 border border-gray-200"
                    >
                      <div
                        className="w-2.5 h-2.5 rounded-full"
                        style={{ backgroundColor: ARM_COLORS[arm] || '#999' }}
                      />
                      <span className="text-xs font-mono">{arm}</span>
                      <span className="text-xs font-medium text-gray-600">
                        {count} ({totalScheduled > 0 ? Math.round((count / totalScheduled) * 100) : 0}%)
                      </span>
                    </div>
                  ))}
              </div>
            </div>
          )}

          {/* Tool call log */}
          {c.tool_call_log.length > 0 && (
            <div>
              <h3 className="text-sm font-semibold text-gray-700 mb-2">Tool Call Sequence</h3>
              <div className="bg-gray-900 rounded-lg p-4 overflow-x-auto max-h-64 overflow-y-auto">
                {c.tool_call_log.map((tc, i) => (
                  <div key={i} className="font-mono text-xs leading-relaxed">
                    <span className="text-gray-500">{i + 1}.</span>{' '}
                    <span className="text-green-400">{tc.tool}</span>
                    <span className="text-gray-400">
                      ({Object.entries(tc.args)
                        .map(([k, v]) => {
                          const s = String(v)
                          return `${k}=${s.length > 30 ? s.slice(0, 30) + '...' : s}`
                        })
                        .join(', ')}
                      )
                    </span>
                    <span className="text-gray-600 ml-2">{tc.duration_ms}ms</span>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Agent report */}
          {c.report && (
            <div>
              <h3 className="text-sm font-semibold text-gray-700 mb-2">Agent Report</h3>
              <div className="bg-gray-50 rounded-lg p-4 text-sm text-gray-700 whitespace-pre-wrap">
                {c.report}
              </div>
            </div>
          )}

          {/* Error */}
          {c.error && (
            <div className="bg-red-50 border border-red-200 rounded-lg p-4 text-sm text-red-700">
              {c.error}
            </div>
          )}
        </div>
      )}
    </div>
  )
}

function Stat({ label, value }: { label: string; value: number }) {
  return (
    <div className="text-center">
      <div className="text-lg font-semibold text-gray-900">{value.toLocaleString()}</div>
      <div className="text-xs text-gray-400">{label}</div>
    </div>
  )
}
