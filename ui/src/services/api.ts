export type Event = {
  event_id: string
  [key: string]: any
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
}
