import { getStoredToken } from "../storage/secureToken";

const DEFAULT_BASE_URL = "https://deltaforge.in/api";
const API_BASE_URL = process.env.EXPO_PUBLIC_API_BASE_URL || DEFAULT_BASE_URL;

export class ApiError extends Error {
  status: number;

  constructor(message: string, status: number) {
    super(message);
    this.status = status;
  }
}

async function request<T>(path: string, tokenOverride?: string): Promise<T> {
  const token = tokenOverride ?? await getStoredToken();
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 15000);

  try {
    const response = await fetch(`${API_BASE_URL}${path}`, {
      headers: {
        Accept: "application/json",
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
      },
      signal: controller.signal,
    });
    const payload = await response.json().catch(() => null);
    if (!response.ok) {
      const message = payload?.detail?.error?.message || payload?.error?.message || `Request failed (${response.status})`;
      throw new ApiError(message, response.status);
    }
    if (payload?.ok === false) {
      throw new ApiError(payload?.error?.message || "Backend returned an error.", response.status);
    }
    return payload as T;
  } finally {
    clearTimeout(timeout);
  }
}

export function getApiBaseUrl() {
  return API_BASE_URL;
}

export function getMobileHealth(token?: string) {
  return request<{ ok: boolean; service: string; timestamp: string; version: string }>("/mobile/health", token);
}

export function getHome() {
  return request<import("./types").HomeResponse>("/mobile/home");
}

export function getSubwallets() {
  return request<import("./types").SubwalletsResponse>("/mobile/subwallets");
}

export function getIronFly() {
  return request<import("./types").IronFlyResponse>("/mobile/iron-fly");
}
