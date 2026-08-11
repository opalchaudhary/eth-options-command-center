import { StyleSheet, Text, View } from "react-native";
import { colors, spacing } from "../theme";

export function MetricRow({ label, value, tone }: { label: string; value: string; tone?: string }) {
  return (
    <View style={styles.row}>
      <Text style={styles.label}>{label}</Text>
      <Text style={[styles.value, tone ? { color: tone } : null]}>{value}</Text>
    </View>
  );
}

const styles = StyleSheet.create({
  row: {
    alignItems: "center",
    borderBottomColor: colors.border,
    borderBottomWidth: StyleSheet.hairlineWidth,
    flexDirection: "row",
    justifyContent: "space-between",
    paddingVertical: spacing.sm,
  },
  label: {
    color: colors.muted,
    fontSize: 13,
  },
  value: {
    color: colors.text,
    flexShrink: 1,
    fontSize: 14,
    fontWeight: "600",
    textAlign: "right",
  },
});
