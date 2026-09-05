import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import type { OrdersPerMinute } from '../types'

interface Props {
  data: OrdersPerMinute[]
}

function formatTime(value: string | number) {
  try {
    if (typeof value === 'string') {
      return new Date(value).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
    }
    return new Date(value).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
  } catch {
    return String(value)
  }
}

export function OrdersChart({ data }: Props) {
  const chartData = data.slice(-30).map(d => ({
    ...d,
    _ts: new Date(d.minute).getTime(),
  }))
  if (chartData.length === 0) {
    return (
      <div className="flex h-48 items-center justify-center text-sm text-slate-500">
        Waiting for stream data…
      </div>
    )
  }

  const maxOrders = Math.max(...chartData.map(d => Number(d.order_count) || 0)) * 1.15

  return (
    <ResponsiveContainer width="100%" height={280}>
      <LineChart data={chartData} margin={{ top: 5, right: 20, left: 0, bottom: 5 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
        <XAxis
          dataKey="_ts"
          type="number"
          domain={['dataMin', 'dataMax']}
          tickFormatter={(ts) => {
            try { return new Date(Number(ts)).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) }
            catch { return String(ts) }
          }}
          tick={{ fontSize: 10, fill: '#94a3b8' }}
          interval="preserveStartEnd"
          allowDuplicatedCategory={false}
        />
        <YAxis
          domain={[0, maxOrders]}
          tick={{ fontSize: 10, fill: '#94a3b8' }}
          tickFormatter={(v) => {
            if (v >= 1000) return `${(v / 1000).toFixed(0)}K`
            return String(v)
          }}
        />
        <Tooltip
          contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #1e293b', borderRadius: '8px', color: '#e2e8f0' }}
          labelFormatter={(v) => `Time: ${formatTime(String(v))}`}
          formatter={(value: any, name: string) => [Number(value).toLocaleString(), name]}
          labelClassName="text-slate-300"
        />
        <Legend wrapperStyle={{ color: '#94a3b8', fontSize: '12px' }} />
        <Line
          type="monotone"
          dataKey="order_count"
          name="Orders"
          stroke="#3b82f6"
          strokeWidth={2}
          dot={false}
          activeDot={{ r: 4 }}
          connectNulls
        />
        <Line
          type="monotone"
          dataKey="failed_count"
          name="Failed"
          stroke="#ef4444"
          strokeWidth={2}
          dot={false}
          activeDot={{ r: 4 }}
          connectNulls
        />
      </LineChart>
    </ResponsiveContainer>
  )
}
