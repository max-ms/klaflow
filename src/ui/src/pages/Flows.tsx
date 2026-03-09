export default function Flows() {
  const flows = [
    { name: 'Welcome Series', trigger: 'New subscriber', steps: 4, status: 'Active' },
    { name: 'Abandoned Cart', trigger: 'Cart abandoned', steps: 3, status: 'Active' },
    { name: 'Win-Back', trigger: '60 days inactive', steps: 5, status: 'Draft' },
  ]

  return (
    <div className="p-8 max-w-7xl mx-auto space-y-6">
      <h1 className="text-2xl font-bold text-gray-900">Flows</h1>

      <div className="bg-amber-50 border border-amber-200 rounded-lg px-4 py-3 text-sm text-amber-800">
        Phase 2: Visual Flow Builder coming soon
      </div>

      <div className="grid grid-cols-3 gap-4">
        {flows.map((flow) => (
          <div key={flow.name} className="bg-white rounded-xl border border-gray-200 p-5 space-y-3">
            <div className="flex items-center justify-between">
              <h3 className="font-semibold text-gray-900">{flow.name}</h3>
              <span
                className={`text-xs px-2 py-0.5 rounded-full font-medium ${
                  flow.status === 'Active'
                    ? 'bg-green-100 text-green-800'
                    : 'bg-gray-100 text-gray-600'
                }`}
              >
                {flow.status}
              </span>
            </div>
            <p className="text-sm text-gray-500">Trigger: {flow.trigger}</p>
            <p className="text-xs text-gray-400">{flow.steps} steps</p>
          </div>
        ))}
      </div>
    </div>
  )
}
