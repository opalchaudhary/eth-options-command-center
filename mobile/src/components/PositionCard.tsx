import { StyleSheet, Text, View } from "react-native";
import { Position } from "../api/types";
import { colors, spacing } from "../theme";
import { formatGreek, formatNumber, formatPrice, pnlColor } from "../utils/formatting";
import { MetricRow } from "./MetricRow";

export function PositionCard({ position }: { position: Position }) {
  const pnlTone = pnlColor(position.unrealized_pnl, colors.positive, colors.negative, colors.text);
  return (
    <View style={styles.card}>
      <Text style={styles.title}>{position.symbol || "Position"}</Text>
      <Text style={styles.subtitle}>{position.contract_type || "NA"}</Text>
      <MetricRow label="Size" value={formatNumber(position.size, 4)} />
      <MetricRow label="Entry" value={formatPrice(position.entry_price)} />
      <MetricRow label="Mark" value={formatPrice(position.mark_price)} />
      <MetricRow label="Unrealized P&L" value={formatPrice(position.unrealized_pnl)} tone={pnlTone} />
      <MetricRow label="Liquidation" value={formatPrice(position.liquidation_price)} />
      <MetricRow label="Margin" value={formatPrice(position.margin)} />
      <MetricRow label="Delta" value={formatGreek(position.computed_delta)} />
      <MetricRow label="Gamma" value={formatGreek(position.computed_gamma)} />
      <MetricRow label="Theta" value={formatGreek(position.computed_theta)} />
      <MetricRow label="Vega" value={formatGreek(position.computed_vega)} />
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
    color: colors.text,
    fontSize: 15,
    fontWeight: "700",
  },
  subtitle: {
    color: colors.muted,
    fontSize: 12,
    marginBottom: spacing.sm,
    marginTop: spacing.xs,
  },
});
