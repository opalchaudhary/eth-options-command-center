import { useCallback, useEffect, useState } from "react";
import { RefreshControl, ScrollView, StyleSheet, Text } from "react-native";
import { getSubwallets } from "../api/client";
import { SubwalletsResponse } from "../api/types";
import { ErrorState } from "../components/ErrorState";
import { GreekCard } from "../components/GreekCard";
import { LoadingState } from "../components/LoadingState";
import { MetricRow } from "../components/MetricRow";
import { StatusCard } from "../components/StatusCard";
import { SubwalletCard } from "../components/SubwalletCard";
import { colors, spacing } from "../theme";
import { formatDate, formatPrice } from "../utils/formatting";

export function SubwalletsScreen() {
  const [data, setData] = useState<SubwalletsResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);

  const load = useCallback(async () => {
    setRefreshing(true);
    setError(null);
    try {
      setData(await getSubwallets());
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to load subwallets.");
    } finally {
      setRefreshing(false);
    }
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  if (!data && refreshing) return <LoadingState label="Loading subwallets" />;

  return (
    <ScrollView
      style={styles.screen}
      contentContainerStyle={styles.content}
      refreshControl={<RefreshControl refreshing={refreshing} onRefresh={load} tintColor={colors.accent} />}
    >
      <Text style={styles.timestamp}>Last updated {formatDate(data?.last_updated)}</Text>
      {error ? <ErrorState message={error} /> : null}
      <StatusCard title="Aggregate" value={formatPrice(data?.aggregate.net_equity)}>
        <MetricRow label="Available" value={formatPrice(data?.aggregate.available_balance)} />
        <MetricRow label="Blocked" value={formatPrice(data?.aggregate.blocked_margin)} />
        <MetricRow label="Position Margin" value={formatPrice(data?.aggregate.position_margin)} />
        <GreekCard greeks={data?.aggregate.greeks} />
      </StatusCard>
      {data?.accounts.map((account) => <SubwalletCard key={account.id} account={account} />)}
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
});
