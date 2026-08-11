import { StyleSheet, View } from "react-native";
import { Greeks } from "../api/types";
import { spacing } from "../theme";
import { formatGreek } from "../utils/formatting";
import { MetricCard } from "./MetricCard";

export function GreekCard({ greeks }: { greeks?: Greeks | null }) {
  return (
    <View style={styles.grid}>
      <MetricCard label="Delta" value={formatGreek(greeks?.delta)} />
      <MetricCard label="Gamma" value={formatGreek(greeks?.gamma)} />
      <MetricCard label="Theta" value={formatGreek(greeks?.theta)} />
      <MetricCard label="Vega" value={formatGreek(greeks?.vega)} />
    </View>
  );
}

const styles = StyleSheet.create({
  grid: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: spacing.sm,
    marginTop: spacing.md,
  },
});
