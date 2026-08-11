import { StyleSheet, Text, View } from "react-native";
import { IronFlyLeg } from "../api/types";
import { colors, spacing } from "../theme";
import { formatGreek, formatNumber, formatPrice } from "../utils/formatting";
import { MetricRow } from "./MetricRow";

export function IronFlyLegCard({ leg }: { leg: IronFlyLeg }) {
  const action = (leg.action || "").toUpperCase();
  const optionType = (leg.option_type || "").replace("_options", "").toUpperCase();
  return (
    <View style={styles.card}>
      <Text style={[styles.title, { color: action === "SELL" ? colors.warning : colors.accent }]}>
        {action || "LEG"} {optionType}
      </Text>
      <MetricRow label="Strike" value={formatPrice(leg.strike)} />
      <MetricRow label="Quantity" value={formatNumber(leg.quantity, 2)} />
      <MetricRow label="Mark" value={formatPrice(leg.mark_price)} />
      <MetricRow label="Open Interest" value={formatNumber(leg.open_interest, 0)} />
      <MetricRow label="Volume" value={formatNumber(leg.volume, 0)} />
      <MetricRow label="IV" value={formatNumber(leg.iv, 2)} />
      <MetricRow label="Delta" value={formatGreek(leg.delta)} />
    </View>
  );
}

const styles = StyleSheet.create({
  card: {
    backgroundColor: colors.surfaceMuted,
    borderRadius: 8,
    marginTop: spacing.md,
    padding: spacing.md,
  },
  title: {
    fontSize: 15,
    fontWeight: "800",
    marginBottom: spacing.sm,
  },
});
