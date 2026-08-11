import { StyleSheet, Text, View } from "react-native";
import { colors, spacing } from "../theme";

export function MetricCard({ label, value }: { label: string; value: string }) {
  return (
    <View style={styles.card}>
      <Text style={styles.label}>{label}</Text>
      <Text style={styles.value}>{value}</Text>
    </View>
  );
}

const styles = StyleSheet.create({
  card: {
    backgroundColor: colors.surfaceMuted,
    borderRadius: 8,
    flex: 1,
    minWidth: "47%",
    padding: spacing.md,
  },
  label: {
    color: colors.muted,
    fontSize: 12,
  },
  value: {
    color: colors.text,
    fontSize: 16,
    fontWeight: "700",
    marginTop: spacing.xs,
  },
});
