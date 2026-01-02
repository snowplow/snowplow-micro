export type Event = {
  event_id: string
  [key: string]: any
}

export type TimelineBucket = {
  start: string  // ISO string
  end: string    // ISO string
}

export type TimelineRequest = {
  buckets: TimelineBucket[]
}

export type TimelinePoint = {
  validEvents: number
  failedEvents: number
  bucket: TimelineBucket
}

export type TimelineData = {
  points: TimelinePoint[]
}

export type ColumnStats = {
  sortable: boolean
  filterable: boolean
  values?: string[]
}

export type ColumnStatsRequest = {
  columns: string[]
}

export type ColumnStatsResponse = Record<string, ColumnStats>

export type EventsFilter = {
  column: string
  value: string
}

export type TimeRange = {
  start?: string  // ISO string
  end?: string    // ISO string
}

export type EventsSorting = {
  column: string
  desc: boolean
}

export type EventsRequest = {
  filters: EventsFilter[]
  validEvents?: boolean // true = valid only, false = failed only, undefined = all
  timeRange?: TimeRange
  sorting?: EventsSorting
  page: number
  pageSize: number
}

export type EventsResponse = {
  events: Event[]
  totalPages: number
  totalItems: number
}

export class EventsApiService {
  /**
   * Make a request with optional authentication
   */
  private static async makeRequest(url: string, options: RequestInit = {}, token?: string | null): Promise<Response> {
    if (token) {
      options.headers = {
        ...options.headers,
        Authorization: `Bearer ${token}`,
      }
    }

    return fetch(url, options)
  }

  /**
   * Fetch filtered events using server-side filtering
   */
  static async fetchFilteredEvents(
    request: EventsRequest,
    token?: string | null
  ): Promise<EventsResponse> {
    const url = new URL('/micro/events', window.location.origin)

    const response = await this.makeRequest(url.toString(), {
      method: 'POST',
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(request),
    }, token)

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`)
    }

    const data = await response.json()
    return data as EventsResponse
  }

  /**
   * Reset all events by calling the /micro/reset endpoint
   */
  static async resetEvents(token?: string | null): Promise<void> {
    const url = new URL('/micro/reset', window.location.origin)

    await this.makeRequest(url.toString(), {
      method: 'POST',
      headers: {
        Accept: 'application/json',
      },
    }, token)
  }

  /**
   * Fetch available column names from the backend
   */
  static async fetchColumns(token?: string | null): Promise<string[]> {
    const url = new URL('/micro/columns', window.location.origin)

    const response = await this.makeRequest(url.toString(), {
      method: 'GET',
      headers: {
        Accept: 'application/json',
      },
    }, token)

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`)
    }

    const data = await response.json()
    return data as string[]
  }

  /**
   * Fetch timeline data from the backend
   */
  static async fetchTimeline(request: TimelineRequest, token?: string | null): Promise<TimelineData> {
    const url = new URL('/micro/timeline', window.location.origin)

    const response = await this.makeRequest(url.toString(), {
      method: 'POST',
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(request),
    }, token)

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`)
    }

    const data = await response.json()
    return data as TimelineData
  }

  /**
   * Fetch column statistics from the backend
   */
  static async fetchColumnStats(
    columns: string[],
    token?: string | null
  ): Promise<ColumnStatsResponse> {
    const url = new URL('/micro/columnStats', window.location.origin)

    const response = await this.makeRequest(url.toString(), {
      method: 'POST',
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ columns }),
    }, token)

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`)
    }

    const data = await response.json()
    return data as ColumnStatsResponse
  }

  /**
   * Generate time buckets for timeline requests
   * @param bucketCount Number of buckets to generate
   * @param bucketSizeMs Size of each bucket in milliseconds
   */
  static generateTimeline(bucketCount: number, bucketSizeMs: number): TimelineRequest {
    const now = new Date()
    // Always align to UTC boundaries for consistency
    const currentTime = now.getTime()
    const alignedTime = Math.floor(currentTime / bucketSizeMs) * bucketSizeMs

    const buckets: TimelineBucket[] = []
    for (let i = 0; i < bucketCount; i++) {
      const bucketStart = alignedTime - i * bucketSizeMs
      const bucketEnd = bucketStart + bucketSizeMs
      buckets.push({
        start: new Date(bucketStart).toISOString(),
        end: new Date(bucketEnd).toISOString()
      })
    }

    return { buckets: buckets.reverse() } // Most recent first
  }

  /**
   * Generate combined timeline request with both minute and weekly buckets
   */
  static generateCombinedTimeline(): TimelineRequest {
    const minuteBuckets = this.generateTimeline(30, 60 * 1000).buckets // 30 buckets, 1 minute each
    const weeklyBuckets = this.generateTimeline(28, 6 * 60 * 60 * 1000).buckets // 28 buckets, 6 hours each

    return {
      buckets: [...minuteBuckets, ...weeklyBuckets]
    }
  }

  /**
   * Split combined timeline response back into minutes and days timelines
   */
  static splitTimelineResponse(response: TimelineData): { minutes: TimelineData, days: TimelineData } {
    const minutePoints = response.points.slice(0, 30) // First 30 are minute buckets
    const dayPoints = response.points.slice(30, 58) // Next 28 are 6-hour buckets (7 days)

    return {
      minutes: { points: minutePoints },
      days: { points: dayPoints }
    }
  }
}
