import { AuthService } from './auth'

export type Event = {
  event_id: string
  [key: string]: any
}

export class EventsApiService {
  /**
   * Make an authenticated request with automatic auth handling
   */
  private static async makeAuthenticatedRequest(url: string, options: RequestInit = {}): Promise<Response> {
    const authService = AuthService.getInstance()

    // Check if auth is enabled
    const authEnabled = await authService.isAuthEnabled()

    if (authEnabled) {
      // If auth is enabled, ensure user is authenticated
      const isAuthenticated = await authService.isAuthenticated()

      if (!isAuthenticated) {
        // Redirect to login if not authenticated
        await authService.login()
        // This will redirect, so we won't reach here
        throw new Error('Redirecting to login')
      }

      // Get access token and add to headers
      const token = await authService.getAccessToken()
      if (token) {
        options.headers = {
          ...options.headers,
          Authorization: `Bearer ${token}`,
        }
      }
    }

    return fetch(url, options)
  }

  /**
   * Fetch events from the snowplow-micro backend
   */
  static async fetchEvents(): Promise<Event[]> {
    const url = new URL('/micro/events', window.location.origin)

    const response = await this.makeAuthenticatedRequest(url.toString(), {
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

    await this.makeAuthenticatedRequest(url.toString(), {
      method: 'POST',
      headers: {
        Accept: 'application/json',
      },
    })
  }
}
