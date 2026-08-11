import { StyleSheet, Text, View } from "react-native";
import { colors, spacing } from "../theme";

export function ErrorState({ message }: { message: string }) {
  return (
    <View style={styles.box}>
      <Text style={styles.title}>Unable to load</Text>
      <Text style={styles.message}>{message}</Text>
    </View>
  );
}

const styles = StyleSheet.create({
  box: {
    backgroundColor: colors.surface,
    borderColor: colors.danger,
    borderRadius: 8,
    borderWidth: 1,
    padding: spacing.lg,
  },
  title: {
    color: colors.danger,
    fontSize: 16,
    fontWeight: "700",
  },
  message: {
    color: colors.text,
    marginTop: spacing.sm,
  },
});
