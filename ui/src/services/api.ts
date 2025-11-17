export type Event = {
  event_id: string
  [key: string]: any
}

export type TimelinePoint = {
  validEvents: number
  failedEvents: number
  timestamp: number
}

export type TimelineData = {
  points: TimelinePoint[]
}

export type ColumnStats = {
  values: string[]
}

export type ColumnStatsRequest = {
  columns: string[]
}

export type EventsFilter = {
  column: string
  value: string
}

export type TimeRange = {
  start?: number
  end?: number
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
  isApproximate: boolean
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
  static async fetchTimeline(token?: string | null): Promise<TimelineData> {
    const url = new URL('/micro/timeline', window.location.origin)

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
    return data as TimelineData
  }

  /**
   * Fetch column statistics from the backend
   */
  static async fetchColumnStats(
    columns: string[],
    token?: string | null
  ): Promise<Record<string, ColumnStats>> {
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
    return data as Record<string, ColumnStats>
  }
}
