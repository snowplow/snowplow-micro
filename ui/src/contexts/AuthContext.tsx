import { createContext, useContext, useEffect, useState, type ReactNode } from 'react'
import { Auth0Client } from '@auth0/auth0-spa-js'

export type AuthConfig = {
  domain: string
  audience: string
  clientId: string
  enabled: boolean
}

type AuthContextType = {
  isAuthenticated: boolean
  isLoading: boolean
  authEnabled: boolean
  getAccessToken: () => Promise<string | null>
  error: string | null
}

const AuthContext = createContext<AuthContextType | undefined>(undefined)

export function AuthProvider({ children }: { children: ReactNode }) {
  const [auth0Client, setAuth0Client] = useState<Auth0Client | null>(null)
  const [authConfig, setAuthConfig] = useState<AuthConfig | null>(null)
  const [isAuthenticated, setIsAuthenticated] = useState(false)
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  // Fetch auth configuration from backend
  const fetchAuthConfig = async (): Promise<AuthConfig> => {
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

    return await response.json() as AuthConfig
  }

  // Initialize authentication
  useEffect(() => {
    const initializeAuth = async () => {
      try {
        setIsLoading(true)
        setError(null)

        // Fetch auth configuration
        const config = await fetchAuthConfig()
        setAuthConfig(config)

        // If auth is disabled, we're done
        if (!config.enabled) {
          setIsAuthenticated(true)
          setIsLoading(false)
          return
        }

        // Initialize Auth0 client
        const client = new Auth0Client({
          domain: config.domain,
          clientId: config.clientId,
          authorizationParams: {
            redirect_uri: window.location.origin + window.location.pathname,
            audience: config.audience,
          },
          useRefreshTokensFallback: true,
          useRefreshTokens: true
        })

        setAuth0Client(client)

        // Handle Auth0 callback if we're returning from login
        const urlParams = new URLSearchParams(window.location.search)
        const handleRedirect = urlParams.has('code') && urlParams.has('state')

        if (handleRedirect) {
          try {
            await client.handleRedirectCallback()
            // Clean up the URL after handling callback
            window.history.replaceState({}, document.title, window.location.pathname)
          } catch (error) {
            console.error('Failed to handle Auth0 callback:', error)
            setError('Authentication callback failed')
            // Clean up URL even on error
            window.history.replaceState({}, document.title, window.location.pathname)
            setIsLoading(false)
            return
          }
        }

        // Check if user is authenticated
        const authenticated = await client.isAuthenticated()
        setIsAuthenticated(authenticated)

        // If not authenticated and not handling a redirect, redirect to login
        if (!authenticated && !handleRedirect) {
          await client.loginWithRedirect()
          return
        }

      } catch (error) {
        console.error('Failed to initialize auth:', error)
        setError(error instanceof Error ? error.message : 'Authentication initialization failed')
      } finally {
        setIsLoading(false)
      }
    }

    initializeAuth()
  }, [])

  // Get access token for API requests
  const getAccessToken = async (): Promise<string | null> => {
    if (!authConfig?.enabled || !auth0Client) {
      return null
    }

    try {
      return await auth0Client.getTokenSilently()
    } catch (error) {
      console.warn('Failed to get access token:', error)
      return null
    }
  }

  const value: AuthContextType = {
    isAuthenticated,
    isLoading,
    authEnabled: authConfig?.enabled || false,
    getAccessToken,
    error,
  }

  return (
    <AuthContext.Provider value={value}>
      {children}
    </AuthContext.Provider>
  )
}

export function useAuth(): AuthContextType {
  const context = useContext(AuthContext)
  if (context === undefined) {
    throw new Error('useAuth must be used within an AuthProvider')
  }
  return context
}