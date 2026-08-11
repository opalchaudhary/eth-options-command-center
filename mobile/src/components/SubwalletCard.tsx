import { useState } from "react";
import { Pressable, StyleSheet, Text, View } from "react-native";
import { SubwalletAccount } from "../api/types";
import { colors, spacing } from "../theme";
import { formatPrice } from "../utils/formatting";
import { GreekCard } from "./GreekCard";
import { MetricRow } from "./MetricRow";
import { PositionCard } from "./PositionCard";

export function SubwalletCard({ account }: { account: SubwalletAccount }) {
  const [expanded, setExpanded] = useState(false);
  return (
    <View style={styles.card}>
      <Pressable onPress={() => setExpanded((value) => !value)} style={styles.header}>
        <View>
          <Text style={styles.title}>{account.label || account.id}</Text>
          <Text style={styles.subtitle}>{account.ok ? "Online" : "Needs attention"}</Text>
        </View>
        <Text style={[styles.status, { color: account.ok ? colors.accent : colors.danger }]}>
          {expanded ? "Hide" : "Details"}
        </Text>
      </Pressable>
      {account.error ? <Text style={styles.error}>{account.error}</Text> : null}
      <MetricRow label="Net Equity" value={formatPrice(account.net_equity)} />
      <MetricRow label="Available" value={formatPrice(account.available_balance)} />
      <MetricRow label="Blocked" value={formatPrice(account.blocked_margin)} />
      <MetricRow label="Position Margin" value={formatPrice(account.position_margin)} />
      <MetricRow label="Positions" value={String(account.position_count)} />
      {expanded ? (
        <>
          <GreekCard greeks={account.greeks} />
          {account.positions.length ? (
            account.positions.map((position, index) => <PositionCard key={`${position.symbol}-${index}`} position={position} />)
          ) : (
            <Text style={styles.empty}>No open positions</Text>
          )}
        </>
      ) : null}
    </View>
  );
}

const styles = StyleSheet.create({
  card: {
    backgroundColor: colors.surface,
    borderColor: colors.border,
    borderRadius: 8,
    borderWidth: 1,
    marginBottom: spacing.md,
    padding: spacing.lg,
  },
  header: {
    alignItems: "center",
    flexDirection: "row",
    justifyContent: "space-between",
    marginBottom: spacing.sm,
  },
  title: {
    color: colors.text,
    fontSize: 17,
    fontWeight: "700",
  },
  subtitle: {
    color: colors.muted,
    fontSize: 12,
    marginTop: spacing.xs,
  },
  status: {
    fontWeight: "700",
  },
  error: {
    color: colors.danger,
    marginBottom: spacing.sm,
  },
  empty: {
    color: colors.muted,
    marginTop: spacing.lg,
  },
});
