from scrapy import Spider, Request
import json
from CrawlEPL.constant import COMP_SEASONS

class PlayerSpider(Spider):
    name = "player"
    custom_settings = {
        'FEEDS': {
            'player.jsonl': {
                'format': 'jsonl'
            }
        }

    }
    def start_requests(self):
        for start_url, season_value, season_key in self.gen_start_urls():
            yield Request(url=start_url, callback=self.parse, meta = {"season_value": season_value, "season": season_key})

    def gen_start_urls(self):
        for season_key, season_value in COMP_SEASONS.items():
            yield f"https://footballapi.pulselive.com/football/players?pageSize=10000&compSeasons={season_value}", season_value, season_key

    def parse(self, response):
        season_data = json.loads(response.text)
        season_value = response.meta["season_value"]
        season = response.meta["season"]

        # for i in range(1, 5):
        #     player_id = str(int(season_data["content"][i]["id"]))
        for _player in season_data['content']:
            player_id = str(int(_player['id']))
            yield response.follow(f"https://footballapi.pulselive.com/football/stats/player/{player_id}?comps=1&compSeasons={season_value}",
                                  meta={"season": season},
                                   callback=self.parse_player)

    def parse_player(self, response):
        player_data = json.loads(response.text)
        player_data['season'] = response.meta['season']
        player_data['type'] = 'player'

        yield player_data
