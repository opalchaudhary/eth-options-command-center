import { StyleSheet, View } from "react-native";
import { IronFlyCandidate } from "../api/types";
import { spacing } from "../theme";
import { formatNumber, formatPrice } from "../utils/formatting";
import { MetricCard } from "./MetricCard";

export function IronFlyCard({ selected }: { selected: IronFlyCandidate | null }) {
  return (
    <View style={styles.grid}>
      <MetricCard label="Expiry" value={selected?.expiry ? new Date(selected.expiry).toLocaleDateString() : "NA"} />
      <MetricCard label="DTE" value={formatNumber(selected?.dte, 2)} />
      <MetricCard label="Center" value={formatPrice(selected?.center_strike)} />
      <MetricCard label="Wing" value={formatNumber(selected?.wing_width, 0)} />
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
