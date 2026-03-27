"""
Network configuration for DOM sync.
Add/remove networks here to control which GAM networks are synced.
"""

NETWORKS = [
    {
        "network_code": "XXXXXXX",
        "name": "Network Name",
        "enabled": True,
    },
    # Add more networks as needed
]


def get_enabled_networks():
    """Return only enabled networks."""
    return [n for n in NETWORKS if n.get("enabled", True)]


def get_network_by_code(code: str):
    """Find network by code."""
    return next((n for n in NETWORKS if n["network_code"] == code), None)
