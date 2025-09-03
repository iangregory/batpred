"""
Simple history cache for Home Assistant data
"""

import threading
from datetime import datetime
from typing import Dict, List, Optional, Any


class HistoryCache:
    """Simple in-memory cache for Home Assistant history data"""

    def __init__(self):
        self.cache_lock = threading.RLock()
        # Cache structure: {entity_id: {"data": [history_items], "latest": datetime}}
        self.cache_data: Dict[str, Dict[str, Any]] = {}
        self.enabled = False

    def configure(self, enabled: bool):
        """Configure the cache"""
        self.enabled = enabled
        if not enabled:
            self.cache_data.clear()

    def _get_timestamp(self, item: Dict[str, Any]) -> Optional[datetime]:
        """Extract timestamp from history item"""
        if not isinstance(item, dict):
            return None

        timestamp_str = item.get("last_changed")
        if timestamp_str:
            try:
                return datetime.fromisoformat(timestamp_str)
            except (ValueError, TypeError):
                pass
        return None

    def get_or_fetch(self, entity_id: str, start_time: datetime, end_time: datetime,
                     fetch_func, minimal: bool = False) -> Optional[List[Dict]]:
        """Get cached data or fetch missing data using provided function"""
        if not self.enabled:
            return fetch_func(start_time, end_time, minimal)

        with self.cache_lock:
            entity_key = entity_id.lower()
            cache_entry = self.cache_data.get(entity_key)

            # Helper function to fetch and update cache
            def fetch_and_update(fetch_start, fetch_end):
                new_data = fetch_func(fetch_start, fetch_end, minimal)
                if new_data:
                    self.update_cache(entity_id, new_data, end_time)
                return new_data

            # Determine what fetch operation is needed
            if not cache_entry:
                return fetch_and_update(start_time, end_time)
            else:
                latest_time = cache_entry.get("latest")
                fetch_and_update(latest_time, end_time)

            # Return filtered cached data
            data = cache_entry["data"]

            # Prune old data in-place
            while data:
                ts = self._get_timestamp(data[0])
                if ts and ts < start_time:
                    data.pop(0)
                else:
                    break

            return [item for item in data
                   if (ts := self._get_timestamp(item)) and ts <= end_time]

    def update_cache(self, entity_id: str, new_data: List[Dict], end_time: datetime):
        """Update cache with new data"""
        if not self.enabled or not new_data:
            return

        # Handle nested list structure from HA API: [[items...]] -> [items...]
        if isinstance(new_data, list) and len(new_data) > 0 and isinstance(new_data[0], list):
            new_data = new_data[0]

        with self.cache_lock:
            entity_key = entity_id.lower()

            if entity_key not in self.cache_data:
                self.cache_data[entity_key] = {"data": [], "latest": None}

            cache_entry = self.cache_data[entity_key]
            existing_data = cache_entry["data"]

            # Get existing timestamps to avoid duplicates
            existing_timestamps = {self._get_timestamp(item) for item in existing_data
                                 if isinstance(item, dict) and self._get_timestamp(item) is not None}

            # Add new items that don't already exist
            latest_time = cache_entry["latest"]
            for item in new_data:
                # Skip if item is not a dict (handles malformed data)
                if not isinstance(item, dict):
                    continue

                timestamp = self._get_timestamp(item)
                if timestamp and timestamp not in existing_timestamps:
                    existing_data.append(item)
                    if latest_time is None or timestamp > latest_time:
                        latest_time = timestamp

            # Sort by timestamp and update latest
            existing_data.sort(key=lambda x: self._get_timestamp(x) or datetime.min)
            cache_entry["latest"] = latest_time
