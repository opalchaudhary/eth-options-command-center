import { ActivityIndicator, StyleSheet, Text, View } from "react-native";
import { colors, spacing } from "../theme";

export function LoadingState({ label = "Loading" }: { label?: string }) {
  return (
    <View style={styles.box}>
      <ActivityIndicator color={colors.accent} />
      <Text style={styles.text}>{label}</Text>
    </View>
  );
}

const styles = StyleSheet.create({
  box: {
    alignItems: "center",
    gap: spacing.md,
    padding: spacing.xl,
  },
  text: {
    color: colors.muted,
  },
});
