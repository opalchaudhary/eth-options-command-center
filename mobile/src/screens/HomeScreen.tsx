import { useCallback, useEffect, useState } from "react";
import { RefreshControl, ScrollView, StyleSheet, Text, View } from "react-native";
import { HomeResponse } from "../api/types";
import { getHome } from "../api/client";
import { ErrorState } from "../components/ErrorState";
import { GreekCard } from "../components/GreekCard";
import { IronFlyCard } from "../components/IronFlyCard";
import { LoadingState } from "../components/LoadingState";
import { MetricRow } from "../components/MetricRow";
import { StatusCard } from "../components/StatusCard";
import { colors, spacing } from "../theme";
import { formatDate, formatPrice } from "../utils/formatting";

export function HomeScreen({ navigation }: any) {
  const [data, setData] = useState<HomeResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);

  const load = useCallback(async () => {
    setRefreshing(true);
    setError(null);
    try {
      setData(await getHome());
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to reach backend.");
    } finally {
      setRefreshing(false);
    }
  }, []);

  useEffect(() => {
    load();
    const id = setInterval(load, 60000);
    return () => clearInterval(id);
  }, [load]);

  if (!data && refreshing) return <LoadingState label="Loading dashboard" />;

  return (
    <ScrollView
      style={styles.screen}
      contentContainerStyle={styles.content}
      refreshControl={<RefreshControl refreshing={refreshing} onRefresh={load} tintColor={colors.accent} />}
    >
      <Text style={styles.title}>DeltaForge</Text>
      <Text style={styles.subtitle}>Last refreshed {formatDate(data?.last_updated)}</Text>
      {error ? <ErrorState message={error} /> : null}
      <StatusCard
        title="Connection"
        value={data?.backend.ok ? "Online" : "Offline"}
        subtitle={data?.market.symbol || "ETHUSD"}
        status={data?.backend.ok ? "ok" : "error"}
      >
        <MetricRow label="ETH price" value={formatPrice(data?.market.spot_price ?? data?.market.mark_price)} />
      </StatusCard>
      <StatusCard title="Aggregate Book" onPress={() => navigation.navigate("Subwallets")} value={formatPrice(data?.subwallets.aggregate.net_equity)}>
        <MetricRow label="Available" value={formatPrice(data?.subwallets.aggregate.available_balance)} />
        <MetricRow label="Blocked" value={formatPrice(data?.subwallets.aggregate.blocked_margin)} />
        <GreekCard greeks={data?.subwallets.aggregate.greeks} />
      </StatusCard>
      <StatusCard title="Subwallets" onPress={() => navigation.navigate("Subwallets")} value={`${data?.subwallets.healthy_account_count ?? 0}/${data?.subwallets.account_count ?? 0} online`}>
        <MetricRow label="Total positions" value={String(data?.subwallets.total_positions ?? 0)} />
      </StatusCard>
      <StatusCard title="Iron Fly" onPress={() => navigation.navigate("Iron Fly")} value={data?.iron_fly.recommendation || "NA"}>
        <MetricRow label="Score" value={String(data?.iron_fly.iron_fly_score ?? "NA")} />
        <MetricRow label="Confidence" value={data?.iron_fly.confidence || "NA"} />
        <IronFlyCard selected={data?.iron_fly.selected as any} />
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
  title: {
    color: colors.text,
    fontSize: 30,
    fontWeight: "800",
  },
  subtitle: {
    color: colors.muted,
    marginBottom: spacing.lg,
    marginTop: spacing.xs,
  },
});
