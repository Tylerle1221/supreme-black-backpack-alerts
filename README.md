# Supreme US backpack alerts
Deploy these files at the root of a Git repository using Render's New Blueprint. Supply TELEGRAM_BOT_TOKEN as a secret. The Blueprint creates one paid worker and a 1 GB persistent disk. Run one instance with Telegram polling enabled (no active Telegram webhook).
Open the Telegram bot in a private chat and press Start. Send /stop to unsubscribe.
The worker checks the SupremeCommunity US restock feed about every 30 seconds. It matches the black Appliqué Flannel Backpack and skips initial-release entries. Alerts refer to reported restocks. Feed delays, outages and very large event bursts can cause delays or missed alerts. SQLite retains subscriptions and queued messages across restarts.
