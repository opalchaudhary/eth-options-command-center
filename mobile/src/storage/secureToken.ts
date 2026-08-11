import * as SecureStore from "expo-secure-store";

const TOKEN_KEY = "deltaforge.mobileApiToken";

export async function getStoredToken() {
  return SecureStore.getItemAsync(TOKEN_KEY);
}

export async function saveToken(token: string) {
  await SecureStore.setItemAsync(TOKEN_KEY, token);
}

export async function clearToken() {
  await SecureStore.deleteItemAsync(TOKEN_KEY);
}
