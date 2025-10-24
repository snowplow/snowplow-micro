import { Auth0Client } from '@auth0/auth0-spa-js'

export interface AuthConfig {
  domain: string
  audience: string
  clientId: string
  enabled: boolean
}

export class AuthService {
  private static instance: AuthService | null = null
  private auth0Client: Auth0Client | null = null
  private authConfig: AuthConfig | null = null
  private isInitialized = false

  private constructor() {}

  static getInstance(): AuthService {
    if (!AuthService.instance) {
      AuthService.instance = new AuthService()
    }
    return AuthService.instance
  }

  /**
   * Fetch auth configuration from the backend
   */
  private async fetchAuthConfig(): Promise<AuthConfig> {
    if (this.authConfig) {
      return this.authConfig
    }

    const url = new URL('/micro/auth-config', window.location.origin)
    const response = await fetch(url.toString(), {
      method: 'GET',
      headers: {
        Accept: 'application/json',
      },
    })

    if (!response.ok) {
      throw new Error(`Failed to fetch auth config: HTTP ${response.status}`)
    }

    const config: AuthConfig = await response.json()
    this.authConfig = config
    return config
  }

  /**
   * Initialize Auth0 client with configuration from backend
   */
  private async initializeAuth0(): Promise<void> {
    if (this.isInitialized) {
      return
    }

    const config = await this.fetchAuthConfig()

    if (!config.enabled) {
      this.isInitialized = true
      return
    }

    this.auth0Client = new Auth0Client({
      domain: config.domain,
      clientId: config.clientId,
      authorizationParams: {
        redirect_uri: window.location.origin + window.location.pathname,
        audience: config.audience,
      },
      useRefreshTokensFallback: true,
      useRefreshTokens: true
    })

    this.isInitialized = true
  }

  /**
   * Check if authentication is enabled
   */
  async isAuthEnabled(): Promise<boolean> {
    const config = await this.fetchAuthConfig()
    return config.enabled
  }

  /**
   * Check if user is authenticated
   */
  async isAuthenticated(): Promise<boolean> {
    await this.initializeAuth0()

    if (!this.auth0Client) {
      return true // Auth disabled, consider user authenticated
    }

    // Handle Auth0 callback if we're returning from login
    const urlParams = new URLSearchParams(window.location.search)
    const handleRedirect = urlParams.has('code') && urlParams.has('state')

    if (handleRedirect) {
      try {
        await this.auth0Client.handleRedirectCallback()

        // Clean up the URL after handling callback
        window.history.replaceState({}, document.title, window.location.pathname)

        // Check if we're actually authenticated after handling the callback
        const isLoggedInAfterRedirect = await this.auth0Client.isAuthenticated()
        if (!isLoggedInAfterRedirect) {
          return false
        }
      } catch (error) {
        console.error('Failed to handle Auth0 callback:', error)
        // Clean up URL even on error
        window.history.replaceState({}, document.title, window.location.pathname)
        return false
      }
    }

    return await this.auth0Client.isAuthenticated()
  }


  /**
   * Get access token for API requests
   */
  async getAccessToken(): Promise<string | null> {
    await this.initializeAuth0()

    if (!this.auth0Client) {
      return null // Auth disabled
    }

    try {
      return await this.auth0Client.getTokenSilently()
    } catch (error) {
      console.warn('Failed to get access token:', error)
      return null
    }
  }

  /**
   * Redirect to Auth0 login
   */
  async login(): Promise<void> {
    await this.initializeAuth0()

    if (!this.auth0Client) {
      return // Auth disabled
    }

    await this.auth0Client.loginWithRedirect()
  }

}