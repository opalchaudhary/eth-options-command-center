import type React from "react";
import { Pressable, StyleSheet, Text, View } from "react-native";
import { colors, spacing } from "../theme";

type Props = {
  title: string;
  value?: string;
  subtitle?: string;
  status?: "ok" | "warning" | "error";
  onPress?: () => void;
  children?: React.ReactNode;
};

export function StatusCard({ title, value, subtitle, status = "ok", onPress, children }: Props) {
  const accent = status === "error" ? colors.danger : status === "warning" ? colors.warning : colors.accent;
  const content = (
    <View style={styles.card}>
      <View style={styles.header}>
        <Text style={styles.title}>{title}</Text>
        <View style={[styles.dot, { backgroundColor: accent }]} />
      </View>
      {value ? <Text style={styles.value}>{value}</Text> : null}
      {subtitle ? <Text style={styles.subtitle}>{subtitle}</Text> : null}
      {children}
    </View>
  );
  if (!onPress) return content;
  return <Pressable onPress={onPress}>{content}</Pressable>;
}

const styles = StyleSheet.create({
  card: {
    backgroundColor: colors.surface,
    borderColor: colors.border,
    borderRadius: 8,
    borderWidth: 1,
    padding: spacing.lg,
    marginBottom: spacing.md,
  },
  header: {
    alignItems: "center",
    flexDirection: "row",
    justifyContent: "space-between",
  },
  title: {
    color: colors.muted,
    fontSize: 13,
    fontWeight: "600",
  },
  value: {
    color: colors.text,
    fontSize: 24,
    fontWeight: "700",
    marginTop: spacing.sm,
  },
  subtitle: {
    color: colors.muted,
    fontSize: 12,
    marginTop: spacing.xs,
  },
  dot: {
    borderRadius: 5,
    height: 10,
    width: 10,
  },
});
