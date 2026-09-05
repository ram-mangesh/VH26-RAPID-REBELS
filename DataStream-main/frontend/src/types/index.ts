export interface OrdersPerMinute {
  minute: string
  order_count: number
  total_revenue: number
  failed_count: number
}

export interface RevenueByRegion {
  region: string
  revenue: number
  orders: number
}

export interface TopProduct {
  product: string
  category: string
  quantity: number
  revenue: number
}

export interface ErrorRate {
  total: number
  failed: number
  error_rate: number
}

export interface GeneratorRate {
  events_per_minute: number
  events_per_second: number
}

export interface MetricsState {
  ordersPerMinute: OrdersPerMinute[]
  revenueByRegion: RevenueByRegion[]
  topProducts: TopProduct[]
  errorRate: ErrorRate | null
  generatorRate: GeneratorRate | null
  loading: boolean
  lastUpdated: Date | null
}
