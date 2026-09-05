import { useState, useEffect, useCallback } from 'react'
import { fetchGeneratorRate, setGeneratorRate, clearPipeline } from '../api/client'

export function RateControl({ onRateChange }: { onRateChange: (rate: number) => void }) {
  const [rate, setRate] = useState<number | null>(null)
  const [setting, setSetting] = useState(false)
  const [feedback, setFeedback] = useState<string>('')
  const [clearing, setClearing] = useState(false)

  const fetchRate = useCallback(async () => {
    try {
      const data = await fetchGeneratorRate()
      setRate(data.events_per_minute)
      onRateChange(data.events_per_minute)
    } catch {
      // ignore
    }
  }, [onRateChange])

  useEffect(() => {
    fetchRate()
    const interval = setInterval(fetchRate, 3000)
    return () => clearInterval(interval)
  }, [fetchRate])

  const showFeedback = (msg: string) => {
    setFeedback(msg)
    setTimeout(() => setFeedback(''), 4000)
  }

  const handleRateChange = async (newRate: number) => {
    setSetting(true)
    try {
      const result = await setGeneratorRate(newRate)
      setRate(result.events_per_minute)
      onRateChange(result.events_per_minute)
      showFeedback(`Rate set to ${(result.events_per_minute / 1000).toFixed(0)}K/min`)
    } catch (err) {
      console.error('Failed to set rate:', err)
      showFeedback('Failed to update rate')
    } finally {
      setSetting(false)
    }
  }

  const handleGradualRamp = async (targetRate: number) => {
    if (setting || clearing) return
    const steps = [1000, 5000, 10000, 20000, targetRate].filter(
      (s) => s > (rate ?? 1000) && s <= targetRate
    )
    if (steps.length === 0) {
      await handleRateChange(targetRate)
      return
    }
    showFeedback(`Ramping up: ${steps.join(' → ')} → ${targetRate / 1000}K`)
    for (const step of steps) {
      await new Promise((resolve) => setTimeout(resolve, 2000))
      await handleRateChange(step)
    }
    setTimeout(async () => {
      await handleRateChange(targetRate)
    }, 500)
  }

  const handleClear = async () => {
    if (!window.confirm('Clear all ClickHouse tables and reset metrics?')) return
    setClearing(true)
    try {
      await clearPipeline()
      showFeedback('Pipeline cleared — generating fresh data...')
      setTimeout(fetchRate, 2000)
    } catch (err) {
      console.error('Failed to clear pipeline:', err)
      showFeedback('Failed to clear pipeline')
    } finally {
      setClearing(false)
    }
  }

  const formatRate = (r: number | null) => {
    if (r === null) return '—'
    if (r >= 100000) return '1L (100K)'
    if (r >= 1000) return `${(r / 1000).toFixed(0)}K`
    return r.toString()
  }

  const presets = [
    { label: '1K', value: 1000, gradual: false },
    { label: '10K', value: 10000, gradual: false },
    { label: '50K', value: 50000, gradual: true },
    { label: '1L', value: 100000, gradual: true },
  ]

  return (
    <div className="flex items-center gap-3 rounded-lg border border-slate-700 bg-slate-800/50 px-4 py-2.5">
      <div className="flex items-center gap-2 text-slate-400">
        <span className="text-xs font-mono text-slate-500">Rate:</span>
        <span className="text-sm font-semibold text-slate-100 font-mono">
          {formatRate(rate)}/min
        </span>
        <span className={`h-1.5 w-1.5 rounded-full ${rate ? 'bg-emerald-500' : 'bg-amber-500 animate-pulse'}`} />
      </div>

      {feedback && (
        <span className={`text-xs font-medium ${feedback.includes('Failed') ? 'text-red-400' : 'text-emerald-400'}`}>
          {feedback}
        </span>
      )}

      <button
        onClick={handleClear}
        disabled={setting || clearing}
        className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-all ${
          clearing
            ? 'bg-red-600 text-white shadow-lg shadow-red-600/30'
            : 'bg-red-700 text-white hover:bg-red-600 disabled:opacity-50'
        }`}
      >
        {clearing ? 'Clearing...' : 'Clear'}
      </button>

      <div className="flex items-center gap-1">
        {presets.map(({ label, value, gradual }) => (
          <button
            key={value}
            onClick={() => gradual ? handleGradualRamp(value) : handleRateChange(value)}
            disabled={setting || rate === value}
            className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-all ${
              rate === value
                ? 'bg-blue-600 text-white shadow-lg shadow-blue-600/30'
                : 'bg-slate-700 text-slate-300 hover:bg-slate-600 hover:text-white disabled:opacity-50'
            }`}
          >
            {label}
            {gradual && <span className="ml-0.5 opacity-50">→</span>}
          </button>
        ))}
      </div>

      {setting && (
        <span className="text-xs text-amber-400 animate-pulse">Updating…</span>
      )}
    </div>
  )
}
