from scrapy import Spider, Request
import json
from CrawlEPL.constant import COMP_SEASONS

class CrawlerSpider(Spider):
    name = "match"

    def start_requests(self):
        for start_url, season, value in self.gen_start_urls():
            yield Request(url=start_url, callback=self.parse, meta={"season": season, "value": value})
    def gen_start_urls(self):
        for k, v in COMP_SEASONS.items():
            yield f"https://footballapi.pulselive.com/football/fixtures?comps=1&compSeasons={v}&page=0&pageSize=2000", k, v

    def parse(self, response):
        data = json.loads(response.text)
        season = response.meta["season"]
        # for i in range(1, 6):
        #     id = str(int(data["content"][i]["id"]))
        for _match in data['content']:
            id = str(int(_match["id"]))
            yield response.follow(f"https://footballapi.pulselive.com/football/fixtures/{id}?altIds=true",
                                  self.parse_match_and_stats, meta={"season": season})

    def parse_match_and_stats(self, response):
        match_data = json.loads(response.text)
        season = response.meta["season"]

        match_id = match_data.get("id")
        stats_url = f"https://footballapi.pulselive.com/football/stats/match/{match_id}"
        yield response.follow(stats_url, self.parse_stats, meta={"match_data": match_data, "season": season})

    def parse_stats(self, response):
        match_data = response.meta.get("match_data")
        season = response.meta["season"]
        stats_data = json.loads(response.text)

        combined_data = self.combine_data(match_data, stats_data)
        combined_data["season"] = season
        combined_data['type'] = 'match'
        yield combined_data

    def combine_data(self, match_data, stats_data):
        combined_data = {
            "id": match_data.get("id"),
            "kick_off": match_data.get("kickoff"),
            "teams": match_data.get("teams"),
            "ground": match_data.get("ground"),
            "clock": match_data.get("clock"),
            "halfTimeScore": match_data.get("halfTimeScore"),
            "teamLists": match_data.get("teamLists"),
            "events": match_data.get("events")["type" == "G" or "type" == "P" or "type" == "O"],
            "stats": stats_data.get("data"),
            }

        return combined_data
