const BASE = '/api'

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: { 'Content-Type': 'application/json' },
    ...init,
  })
  if (!res.ok) {
    const detail = await res.text().catch(() => res.statusText)
    throw new Error(`${res.status}: ${detail}`)
  }
  return res.json()
}

// Dashboard
export const fetchDashboardStats = () => request<{
  total_profiles: number
  active_segments: number
  events_24h: number
  revenue_7d: number
}>('/dashboard/stats')

// Segments
export const fetchSegments = () => request<{
  segment_name: string
  member_count: string
}[]>('/segments')

export const fetchSegmentDetail = (name: string) => request<{
  segment_name: string
  member_count: number
  total_profiles: number
  percentage: number
  condition: string
}>(`/segments/${name}/detail`)

export const fetchSegmentCustomers = (name: string, limit = 100) =>
  request<{ account_id: string; customer_id: string; evaluated_at: string }[]>(
    `/segments/${name}/customers?limit=${limit}`,
  )

// Customers
export const fetchCustomers = (page: number, search = '') =>
  request<{
    customers: {
      customer_id: string
      account_id: string
      lifecycle_state: string
      clv_tier: string
      churn_risk_score: number
      updated_at: string
    }[]
    total: number
    page: number
    page_size: number
  }>(`/customers?page=${page}&page_size=50&search=${encodeURIComponent(search)}`)

export const fetchCustomerSegments = (id: string) =>
  request<{ segment_name: string; in_segment: number | string; evaluated_at: string }[]>(
    `/customers/${id}/segments`,
  )

export const fetchCustomerAllSegments = (id: string) =>
  request<{ segment_name: string; in_segment: number | string; evaluated_at: string }[]>(
    `/customers/${id}/all-segments`,
  )

export const fetchCustomerFeatures = (id: string) =>
  request<Record<string, string>>(`/customers/${id}/features`)

export const fetchCustomerScores = (id: string) =>
  request<{ clv_tier: string; churn_risk_score: number; discount_sensitivity: number }>(
    `/customers/${id}/scores`,
  )

export const fetchCustomerEvents = (id: string, page = 1) =>
  request<{
    events: { metric_name: string; metric_value: string; computed_at: string }[]
    total: number
    page: number
    page_size: number
  }>(`/customers/${id}/events?page=${page}`)

// Decisions
export const postDecide = (customer_id: string, account_id: string, context?: string) =>
  request<{ arm: string; score: number; reasoning: Record<string, unknown> }>('/decide', {
    method: 'POST',
    body: JSON.stringify({ customer_id, account_id, context }),
  })

export const fetchDecisionHistory = (customer_id: string) =>
  request<{
    account_id: string
    customer_id: string
    arm_chosen: string
    feature_snapshot: string
    created_at: string
    reward: string
  }[]>(`/decisions/${customer_id}/history`)

export const fetchRecentDecisions = (limit = 20) =>
  request<{
    account_id: string
    customer_id: string
    arm_chosen: string
    created_at: string
  }[]>(`/decisions/recent?limit=${limit}`)

export const fetchArmStats = () =>
  request<Record<string, {
    weights: number[]
    alpha: number
    beta: number
    expected_thompson: number
  }>>('/bandit/arm-stats')

// Accounts
export const fetchAccountMetrics = (id: string) =>
  request<{ metric_name: string; metric_value: string; computed_at: string }[]>(
    `/accounts/${id}/metrics`,
  )
