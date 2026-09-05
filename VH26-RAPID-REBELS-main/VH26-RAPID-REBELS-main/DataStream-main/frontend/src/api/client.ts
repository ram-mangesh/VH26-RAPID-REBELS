import axios from 'axios'
import type { ErrorRate, OrdersPerMinute, RevenueByRegion, TopProduct, GeneratorRate, RealtimeRate } from '../types'

const api = axios.create({
  baseURL: '/api',
  timeout: 10_000,
})

export async function fetchOrdersPerMinute(): Promise<OrdersPerMinute[]> {
  const { data } = await api.get<OrdersPerMinute[]>('/metrics/events-per-minute')
  return data ?? []
}

export async function fetchRevenueByRegion(): Promise<RevenueByRegion[]> {
  const { data } = await api.get<RevenueByRegion[]>('/metrics/revenue-by-region')
  return data ?? []
}

export async function fetchTopProducts(): Promise<TopProduct[]> {
  const { data } = await api.get<TopProduct[]>('/metrics/top-products')
  return data ?? []
}

export async function fetchErrorRate(): Promise<ErrorRate> {
  const { data } = await api.get<ErrorRate>('/metrics/error-rate')
  return data
}

export async function fetchGeneratorRate(): Promise<GeneratorRate> {
  const { data } = await api.get<GeneratorRate>('/generator/rate')
  return data
}

export async function setGeneratorRate(rate: number): Promise<GeneratorRate> {
  const { data } = await api.post<GeneratorRate>('/generator/rate', { events_per_minute: rate })
  return data
}

export async function clearPipeline(): Promise<{ success: boolean; message: string }> {
  const { data } = await api.post('/pipeline/clear')
  return data
}

export async function fetchRealtimeRate(): Promise<RealtimeRate> {
  const { data } = await api.get<RealtimeRate>('/metrics/realtime-rate')
  return data
}

const intelApi = axios.create({
  baseURL: '/intel-api',
  timeout: 10_000,
})

export async function resetIntelligencePipeline(): Promise<{ ok: boolean }> {
  const { data } = await intelApi.post('/api/reset')
  return data
}
