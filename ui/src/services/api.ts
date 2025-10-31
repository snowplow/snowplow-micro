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

export class EventsApiService {
  /**
   * Fetch events from the snowplow-micro backend
   */
  static async fetchEvents(): Promise<Event[]> {
    const url = new URL('/micro/events', window.location.origin)

    const response = await fetch(url.toString(), {
      method: 'GET',
      headers: {
        Accept: 'application/json',
      },
    })

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`)
    }

    const data = await response.json()
    return data as Event[]
  }

  /**
   * Reset all events by calling the /micro/reset endpoint
   */
  static async resetEvents(): Promise<void> {
    const url = new URL('/micro/reset', window.location.origin)

    await fetch(url.toString(), {
      method: 'POST',
      headers: {
        Accept: 'application/json',
      },
    })
  }

  /**
   * Fetch available column names from the backend
   */
  static async fetchColumns(): Promise<string[]> {
    const url = new URL('/micro/columns', window.location.origin)

    const response = await fetch(url.toString(), {
      method: 'GET',
      headers: {
        Accept: 'application/json',
      },
    })

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`)
    }

    const data = await response.json()
    return data as string[]
  }

  /**
   * Fetch timeline data from the backend
   */
  static async fetchTimeline(): Promise<TimelineData> {
    const url = new URL('/micro/timeline', window.location.origin)

    const response = await fetch(url.toString(), {
      method: 'GET',
      headers: {
        Accept: 'application/json',
      },
    })

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`)
    }

    const data = await response.json()
    return data as TimelineData
  }

  /**
   * Fetch column statistics from the backend
   */
  static async fetchColumnStats(columns: string[]): Promise<Record<string, ColumnStats>> {
    const url = new URL('/micro/columnStats', window.location.origin)

    const response = await fetch(url.toString(), {
      method: 'POST',
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ columns }),
    })

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`)
    }

    const data = await response.json()
    return data as Record<string, ColumnStats>
  }
}
