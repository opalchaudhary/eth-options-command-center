import { useState } from "react";
import { Alert, Pressable, StyleSheet, Text, TextInput, View } from "react-native";
import { getApiBaseUrl, getMobileHealth } from "../api/client";
import { clearToken, saveToken } from "../storage/secureToken";
import { colors, spacing } from "../theme";

type Props = {
  setupMode?: boolean;
  onTokenChanged?: () => void;
};

export function SettingsScreen({ setupMode = false, onTokenChanged }: Props) {
  const [token, setToken] = useState("");
  const [testing, setTesting] = useState(false);
  const [message, setMessage] = useState<string | null>(null);

  async function testConnection(candidate = token) {
    setTesting(true);
    setMessage(null);
    try {
      await getMobileHealth(candidate.trim());
      setMessage("Connection verified.");
      return true;
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "Connection failed.");
      return false;
    } finally {
      setTesting(false);
    }
  }

  async function save() {
    const trimmed = token.trim();
    if (!trimmed) {
      setMessage("Enter the mobile API token first.");
      return;
    }
    const ok = await testConnection(trimmed);
    if (!ok) return;
    await saveToken(trimmed);
    setToken("");
    onTokenChanged?.();
  }

  async function clear() {
    await clearToken();
    setMessage("Token cleared.");
    onTokenChanged?.();
  }

  return (
    <View style={styles.screen}>
      <Text style={styles.title}>DeltaForge</Text>
      <Text style={styles.subtitle}>{setupMode ? "Enter your private mobile API token." : "Mobile API settings"}</Text>
      <View style={styles.card}>
        <Text style={styles.label}>API base URL</Text>
        <Text style={styles.url}>{getApiBaseUrl()}</Text>
        <Text style={styles.label}>Mobile API token</Text>
        <TextInput
          autoCapitalize="none"
          autoCorrect={false}
          placeholder="Bearer token"
          placeholderTextColor={colors.muted}
          secureTextEntry
          value={token}
          onChangeText={setToken}
          style={styles.input}
        />
        <Pressable style={styles.button} onPress={save} disabled={testing}>
          <Text style={styles.buttonText}>{testing ? "Testing..." : "Save token"}</Text>
        </Pressable>
        <Pressable style={styles.secondaryButton} onPress={() => testConnection()} disabled={testing}>
          <Text style={styles.secondaryText}>Test connection</Text>
        </Pressable>
        {!setupMode ? (
          <Pressable
            style={styles.clearButton}
            onPress={() => Alert.alert("Clear token", "Remove the stored mobile token?", [{ text: "Cancel" }, { text: "Clear", onPress: clear }])}
          >
            <Text style={styles.clearText}>Clear token</Text>
          </Pressable>
        ) : null}
        {message ? <Text style={styles.message}>{message}</Text> : null}
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  screen: {
    backgroundColor: colors.background,
    flex: 1,
    padding: spacing.xl,
    justifyContent: "center",
  },
  title: {
    color: colors.text,
    fontSize: 32,
    fontWeight: "800",
  },
  subtitle: {
    color: colors.muted,
    marginTop: spacing.sm,
  },
  card: {
    backgroundColor: colors.surface,
    borderColor: colors.border,
    borderRadius: 8,
    borderWidth: 1,
    marginTop: spacing.xl,
    padding: spacing.lg,
  },
  label: {
    color: colors.muted,
    fontSize: 12,
    marginBottom: spacing.xs,
    marginTop: spacing.md,
  },
  url: {
    color: colors.text,
  },
  input: {
    borderColor: colors.border,
    borderRadius: 8,
    borderWidth: 1,
    color: colors.text,
    padding: spacing.md,
  },
  button: {
    alignItems: "center",
    backgroundColor: colors.accent,
    borderRadius: 8,
    marginTop: spacing.lg,
    padding: spacing.md,
  },
  buttonText: {
    color: colors.background,
    fontWeight: "800",
  },
  secondaryButton: {
    alignItems: "center",
    borderColor: colors.border,
    borderRadius: 8,
    borderWidth: 1,
    marginTop: spacing.md,
    padding: spacing.md,
  },
  secondaryText: {
    color: colors.text,
    fontWeight: "700",
  },
  clearButton: {
    alignItems: "center",
    marginTop: spacing.md,
    padding: spacing.md,
  },
  clearText: {
    color: colors.danger,
    fontWeight: "700",
  },
  message: {
    color: colors.text,
    marginTop: spacing.md,
  },
});
