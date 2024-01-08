# pipelines.py

import os
import json
from scrapy.exporters import JsonItemExporter
from scrapy.exceptions import DropItem

class MatchPipeline:
    def open_spider(self, spider):
        self.match_items = {}

    def process_item(self, item, spider):
        item_type = item.get('type')
        if item_type != 'match':
            return item

        season = item.get('season')
        if not season:
            raise DropItem("Match item doesn't have a season field")

        if season not in self.match_items:
            self.match_items[season] = []

        self.match_items[season].append(item)
        return item

    def close_spider(self, spider):
        output_folder = 'output'

        for season, items in self.match_items.items():
            folder_path = os.path.join(output_folder, season)
            os.makedirs(folder_path, exist_ok=True)

            filename = os.path.join(folder_path, f'matches_data.json')

            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(items, f, ensure_ascii=False, indent=2)


class PlayerPipeline:
    def open_spider(self, spider):
        self.player_items = {}

    def process_item(self, item, spider):
        item_type = item.get('type')
        if item_type != 'player':
            return item

        season = item.get('season')
        if not season:
            raise DropItem("Player item doesn't have a season field")

        if season not in self.player_items:
            self.player_items[season] = []

        self.player_items[season].append(item)
        return item

    def close_spider(self, spider):
        output_folder = 'output'

        for season, items in self.player_items.items():
            folder_path = os.path.join(output_folder, season)
            os.makedirs(folder_path, exist_ok=True)

            filename = os.path.join(folder_path, f'players_data.json')

            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(items, f, ensure_ascii=False, indent=2)
