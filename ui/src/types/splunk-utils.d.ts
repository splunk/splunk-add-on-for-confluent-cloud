declare module '@splunk/splunk-utils/config' {
  export const app: string;
  export const locale: string;
  export const root: string;
  export const CSRFToken: string;
  export const make_url: (path: string) => string;
  export const build_api_url: (path: string) => string;
}

declare module '@splunk/splunk-utils/url' {
  export function createRESTURL(path: string, params?: Record<string, any>): string;
  export function createURL(path: string, params?: Record<string, any>): string;
}

declare module '@splunk/splunk-utils/fetch' {
  export interface FetchOptions {
    method?: string;
    headers?: Record<string, string>;
    body?: string;
    credentials?: 'include' | 'omit' | 'same-origin';
  }
  
  export function handleRequest(url: string, options?: FetchOptions): Promise<Response>;
  export function handleResponse(expectedStatus: number): (response: Response) => Promise<any>;
  export function handleError(error: Error): void;
  export const defaultFetchInit: FetchOptions;
}