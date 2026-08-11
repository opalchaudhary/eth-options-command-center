import { useCallback, useEffect, useState } from "react";
import { RefreshControl, ScrollView, StyleSheet, Text, View } from "react-native";
import { getIronFly } from "../api/client";
import { IronFlyResponse } from "../api/types";
import { ErrorState } from "../components/ErrorState";
import { GreekCard } from "../components/GreekCard";
import { IronFlyCard } from "../components/IronFlyCard";
import { IronFlyLegCard } from "../components/IronFlyLegCard";
import { LoadingState } from "../components/LoadingState";
import { MetricRow } from "../components/MetricRow";
import { StatusCard } from "../components/StatusCard";
import { colors, spacing } from "../theme";
import { formatDate, formatNumber, formatPct, formatPrice } from "../utils/formatting";

export function IronFlyScreen() {
  const [data, setData] = useState<IronFlyResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);

  const load = useCallback(async () => {
    setRefreshing(true);
    setError(null);
    try {
      setData(await getIronFly());
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to load Iron Fly.");
    } finally {
      setRefreshing(false);
    }
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  const selected = data?.selected ?? null;
  const payoff = selected?.payoff;

  if (!data && refreshing) return <LoadingState label="Loading Iron Fly" />;

  return (
    <ScrollView
      style={styles.screen}
      contentContainerStyle={styles.content}
      refreshControl={<RefreshControl refreshing={refreshing} onRefresh={load} tintColor={colors.accent} />}
    >
      <Text style={styles.timestamp}>Generated {formatDate(data?.generated_at)}</Text>
      {error ? <ErrorState message={error} /> : null}
      <StatusCard title="Recommendation" value={data?.recommendation || "NA"} subtitle={`Score ${data?.iron_fly_score ?? "NA"} - ${data?.confidence || "NA"}`}>
        <IronFlyCard selected={selected} />
      </StatusCard>
      <StatusCard title="Payoff">
        <MetricRow label="Net Credit" value={formatPrice(payoff?.net_credit)} />
        <MetricRow label="Max Profit" value={formatPrice(payoff?.max_profit)} />
        <MetricRow label="Max Loss" value={formatPrice(payoff?.max_loss)} />
        <MetricRow label="Return on Risk" value={formatPct(payoff?.return_on_risk)} />
        <MetricRow label="Lower Breakeven" value={formatPrice(payoff?.lower_breakeven)} />
        <MetricRow label="Upper Breakeven" value={formatPrice(payoff?.upper_breakeven)} />
      </StatusCard>
      <StatusCard title="Greeks">
        <GreekCard greeks={selected?.net_greeks} />
      </StatusCard>
      <StatusCard title="Market Context">
        <MetricRow label="Expected Move" value={formatPrice(selected?.expected_move)} />
        <MetricRow label="Median IV" value={formatNumber(selected?.median_iv, 2)} />
        <MetricRow label="Realized Vol" value={formatNumber(selected?.realized_vol_pct, 2)} />
        <MetricRow label="IV-RV Spread" value={formatNumber(selected?.iv_rv_spread, 2)} />
        <MetricRow label="Liquidity" value={formatNumber(selected?.liquidity_score, 2)} />
      </StatusCard>
      <StatusCard title="Legs">
        {selected?.legs?.length ? selected.legs.map((leg, index) => <IronFlyLegCard key={`${leg.action}-${leg.option_type}-${index}`} leg={leg} />) : <Text style={styles.empty}>No selected legs</Text>}
      </StatusCard>
      <StatusCard title="Alternatives">
        {data?.top_alternatives?.length ? data.top_alternatives.map((item, index) => (
          <View key={`${item.expiry}-${index}`} style={styles.alt}>
            <MetricRow label="Expiry" value={item.expiry ? new Date(item.expiry).toLocaleDateString() : "NA"} />
            <MetricRow label="Score" value={formatNumber(item.score, 2)} />
            <MetricRow label="Center" value={formatPrice(item.center_strike)} />
            <MetricRow label="Wing" value={formatNumber(item.wing_width, 0)} />
          </View>
        )) : <Text style={styles.empty}>No alternatives ranked</Text>}
      </StatusCard>
      <StatusCard title="Rules / Risk">
        <Text style={styles.section}>Entry Conditions</Text>
        {(data?.entry_conditions || []).map((item, index) => <Text key={`entry-${index}`} style={styles.text}>{item}</Text>)}
        <Text style={styles.section}>Adjustment Triggers</Text>
        {(data?.adjustment_triggers || []).map((item, index) => <Text key={`adjust-${index}`} style={styles.text}>{item}</Text>)}
        <Text style={styles.section}>Stop Loss</Text>
        <Text style={styles.text}>{data?.stop_loss_logic || "NA"}</Text>
        <Text style={styles.section}>Profit Booking</Text>
        <Text style={styles.text}>{data?.profit_booking_logic || "NA"}</Text>
        <Text style={styles.section}>Time Exit</Text>
        <Text style={styles.text}>{data?.time_based_exit || "NA"}</Text>
        <Text style={styles.section}>Risk Factors</Text>
        {(data?.risk_factors || []).map((item, index) => <Text key={`risk-${index}`} style={styles.text}>{item}</Text>)}
      </StatusCard>
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  screen: {
    backgroundColor: colors.background,
    flex: 1,
  },
  content: {
    padding: spacing.lg,
  },
  timestamp: {
    color: colors.muted,
    marginBottom: spacing.md,
  },
  empty: {
    color: colors.muted,
  },
  alt: {
    backgroundColor: colors.surfaceMuted,
    borderRadius: 8,
    marginTop: spacing.md,
    padding: spacing.md,
  },
  section: {
    color: colors.text,
    fontWeight: "800",
    marginTop: spacing.md,
  },
  text: {
    color: colors.muted,
    lineHeight: 20,
    marginTop: spacing.xs,
  },
});
