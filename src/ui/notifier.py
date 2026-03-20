"""
DiscordReporter — sends mint success and NFT transfer notifications via Discord Webhook.
"""

import aiohttp
from src.config.settings import ConfigurationManager


class DiscordReporter:
    @staticmethod
    async def _post(payload: dict):
        cfg = ConfigurationManager()
        if not cfg.discord_enabled or not cfg.webhook_url:
            return
        try:
            async with aiohttp.ClientSession() as session:
                await session.post(cfg.webhook_url, json=payload)
        except Exception:
            pass

    @classmethod
    async def notify_success(cls, worker_id: int, wallet: str, tx_hash: str, qty: int, network: str):
        await cls._post({
            "embeds": [{
                "title": "✅ Mint Successful!",
                "color": 0x00FF88,
                "fields": [
                    {"name": "Worker", "value": f"#{worker_id}", "inline": True},
                    {"name": "Network", "value": network, "inline": True},
                    {"name": "Quantity", "value": str(qty), "inline": True},
                    {"name": "Wallet", "value": f"`{wallet[:8]}...{wallet[-4:]}`", "inline": False},
                    {"name": "TX Hash", "value": f"`{tx_hash}`", "inline": False},
                ],
            }]
        })

    @classmethod
    async def notify_transfer(cls, worker_id: int, wallet: str, tx_hash: str, recipient: str):
        await cls._post({
            "embeds": [{
                "title": "📦 NFT Transferred",
                "color": 0x7C3AED,
                "fields": [
                    {"name": "Worker", "value": f"#{worker_id}", "inline": True},
                    {"name": "From", "value": f"`{wallet[:8]}...{wallet[-4:]}`", "inline": True},
                    {"name": "To", "value": f"`{recipient[:8]}...{recipient[-4:]}`", "inline": True},
                    {"name": "TX Hash", "value": f"`{tx_hash}`", "inline": False},
                ],
            }]
        })
