# DeltaForge Mobile

Private read-only Android monitoring app for DeltaForge.

## Requirements

- Node.js compatible with the current Expo SDK
- Expo Go on your Android phone
- A server-side `MOBILE_API_TOKEN`

The app uses `https://deltaforge.in/api` by default. Override it with:

```bash
EXPO_PUBLIC_API_BASE_URL=https://deltaforge.in/api
```

## Install And Run With Expo Go

```bash
cd mobile
npm install
npx expo install expo-secure-store expo-status-bar react-native-safe-area-context react-native-screens
npm run start
```

Then:

1. Open Expo Go on Android.
2. Scan the QR code.
3. Enter the private mobile API token.
4. Tap `Test connection`.
5. Save the token after the connection succeeds.

## Type Check

```bash
cd mobile
npm run typecheck
```

## Build A Private Android APK

```bash
cd mobile
npm install
npx eas login
npx eas build:configure
npx eas build --platform android --profile preview
```

The `preview` profile in `eas.json` is configured with `android.buildType = "apk"`.

Do not publish or submit this app unless you intentionally create a release process later.
