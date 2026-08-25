from .config import TestnetEndpointConfig, validate_testnet_endpoints
from .delta_testnet_client import DeltaTestnetClient


class DeltaGridWebSocketConfig:
    def __init__(self, endpoints: TestnetEndpointConfig | None = None):
        self.endpoints = endpoints or TestnetEndpointConfig()
        validate_testnet_endpoints(self.endpoints)


def private_auth_message(client: DeltaTestnetClient) -> dict:
    return client.websocket_auth_payload()

