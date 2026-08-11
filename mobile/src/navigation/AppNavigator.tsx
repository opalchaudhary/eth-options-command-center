import { NavigationContainer, DarkTheme } from "@react-navigation/native";
import { createBottomTabNavigator } from "@react-navigation/bottom-tabs";
import { useEffect, useState } from "react";
import { LoadingState } from "../components/LoadingState";
import { HomeScreen } from "../screens/HomeScreen";
import { IronFlyScreen } from "../screens/IronFlyScreen";
import { SettingsScreen } from "../screens/SettingsScreen";
import { SubwalletsScreen } from "../screens/SubwalletsScreen";
import { getStoredToken } from "../storage/secureToken";
import { colors } from "../theme";

export type RootTabParamList = {
  Home: undefined;
  Subwallets: undefined;
  "Iron Fly": undefined;
  Settings: undefined;
};

const Tab = createBottomTabNavigator<RootTabParamList>();

const navTheme = {
  ...DarkTheme,
  colors: {
    ...DarkTheme.colors,
    background: colors.background,
    card: colors.surface,
    border: colors.border,
    primary: colors.accent,
    text: colors.text,
  },
};

export function AppNavigator() {
  const [tokenReady, setTokenReady] = useState(false);
  const [hasToken, setHasToken] = useState(false);

  async function refreshTokenState() {
    const token = await getStoredToken();
    setHasToken(Boolean(token));
    setTokenReady(true);
  }

  useEffect(() => {
    refreshTokenState();
  }, []);

  if (!tokenReady) return <LoadingState label="Preparing DeltaForge" />;

  if (!hasToken) {
    return (
      <SettingsScreen
        setupMode
        onTokenChanged={refreshTokenState}
      />
    );
  }

  return (
    <NavigationContainer theme={navTheme}>
      <Tab.Navigator
        screenOptions={{
          headerStyle: { backgroundColor: colors.surface },
          headerTintColor: colors.text,
          tabBarActiveTintColor: colors.accent,
          tabBarInactiveTintColor: colors.muted,
          tabBarStyle: { backgroundColor: colors.surface, borderTopColor: colors.border },
        }}
      >
        <Tab.Screen name="Home" component={HomeScreen} />
        <Tab.Screen name="Subwallets" component={SubwalletsScreen} />
        <Tab.Screen name="Iron Fly" component={IronFlyScreen} />
        <Tab.Screen name="Settings">
          {() => <SettingsScreen onTokenChanged={refreshTokenState} />}
        </Tab.Screen>
      </Tab.Navigator>
    </NavigationContainer>
  );
}
