const lifecycleColors: Record<string, string> = {
  new: 'bg-blue-100 text-blue-800',
  active: 'bg-green-100 text-green-800',
  at_risk: 'bg-amber-100 text-amber-800',
  lapsed: 'bg-orange-100 text-orange-800',
  churned: 'bg-red-100 text-red-800',
  unknown: 'bg-gray-100 text-gray-600',
}

const clvColors: Record<string, string> = {
  low: 'bg-gray-100 text-gray-700',
  medium: 'bg-blue-100 text-blue-700',
  high: 'bg-purple-100 text-purple-700',
  vip: 'bg-yellow-100 text-yellow-800',
  unknown: 'bg-gray-100 text-gray-600',
}

export function LifecycleBadge({ state }: { state: string }) {
  const color = lifecycleColors[state] || lifecycleColors.unknown
  return (
    <span className={`inline-block px-2 py-0.5 rounded-full text-xs font-medium ${color}`}>
      {state.replace('_', ' ')}
    </span>
  )
}

export function ClvBadge({ tier }: { tier: string }) {
  const color = clvColors[tier] || clvColors.unknown
  return (
    <span className={`inline-block px-2 py-0.5 rounded-full text-xs font-medium uppercase ${color}`}>
      {tier}
    </span>
  )
}

export function SegmentPill({ name }: { name: string }) {
  return (
    <span className="inline-block px-2 py-0.5 rounded-full text-xs font-medium bg-klaviyo-green/10 text-klaviyo-green-dark border border-klaviyo-green/20">
      {name.replace(/_/g, ' ')}
    </span>
  )
}
