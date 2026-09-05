import { useState } from 'react'
import { setGeneratorRate, clearPipeline } from '../api/client'

export function RateControl({ initialRate, onRateChange }: { 
  initialRate: number 
  onRateChange: (rate: number) => void 
}) {
  const [rate, setRate] = useState(initialRate)
  const [setting, setSetting] = useState(false)

  const handleRateChange = async (newRate: number) => {
    setSetting(true)
    try {
      const result = await setGeneratorRate(newRate)
      setRate(result.events_per_minute)
      onRateChange(result.events_per_minute)
    } catch (err) {
      console.error('Failed to set rate:', err)
    } finally {
      setSetting(false)
    }
  }

  const handleClear = async () => {
    setSetting(true)
    try {
      await clearPipeline()
      setRate(1000)
      onRateChange(1000)
    } catch (err) {
      console.error('Failed to clear pipeline:', err)
    } finally {
      setTimeout(() => setSetting(false), 1000)
    }
  }

  const formatRate = (r: number) => {
    if (r >= 100000) return '1L (100K)'
    if (r >= 1000) return `${(r / 1000).toFixed(0)}K`
    return r.toString()
  }

  return (
    <div className="flex items-center gap-3 rounded-lg border border-slate-700 bg-slate-800/50 px-4 py-2.5">
      <div className="flex items-center gap-2 text-slate-400">
        <span className="text-xs font-mono text-slate-500">Rate:</span>
        <span className="text-sm font-semibold text-slate-100 font-mono">
          {formatRate(rate)}/min
        </span>
        <span className="h-1.5 w-1.5 rounded-full bg-emerald-500" />
      </div>
      <button
        onClick={handleClear}
        disabled={setting}
        className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-all ${
          setting
            ? 'bg-red-600 text-white shadow-lg shadow-red-600/30'
            : 'bg-red-700 text-white hover:bg-red-600 disabled:opacity-50'
        }`}
      >
        Clear
      </button>
      <div className="flex items-center gap-1 ml-2">
        <button
          onClick={() => handleRateChange(1000)}
          disabled={setting || rate === 1000}
          className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-all ${
            rate === 1000
              ? 'bg-blue-600 text-white shadow-lg shadow-blue-600/30'
              : 'bg-slate-700 text-slate-300 hover:bg-slate-600 hover:text-white disabled:opacity-50'
          }`}
        >
          1K
        </button>
        <button
          onClick={() => handleRateChange(10000)}
          disabled={setting || rate === 10000}
          className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-all ${
            rate === 10000
              ? 'bg-blue-600 text-white shadow-lg shadow-blue-600/30'
              : 'bg-slate-700 text-slate-300 hover:bg-slate-600 hover:text-white disabled:opacity-50'
          }`}
        >
          10K
        </button>
        <button
          onClick={() => handleRateChange(50000)}
          disabled={setting || rate === 50000}
          className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-all ${
            rate === 50000
              ? 'bg-blue-600 text-white shadow-lg shadow-blue-600/30'
              : 'bg-slate-700 text-slate-300 hover:bg-slate-600 hover:text-white disabled:opacity-50'
          }`}
        >
          50K
        </button>
        <button
          onClick={() => handleRateChange(100000)}
          disabled={setting || rate === 100000}
          className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-all ${
            rate === 100000
              ? 'bg-blue-600 text-white shadow-lg shadow-blue-600/30'
              : 'bg-slate-700 text-slate-300 hover:bg-slate-600 hover:text-white disabled:opacity-50'
          }`}
        >
          1L
        </button>
      </div>
      {setting && (
        <span className="text-xs text-amber-400 animate-pulse">Updating…</span>
      )}
    </div>
  )
}