# Job Watcher Bot

A lightweight Python bot that watches multiple career sites, filters matching roles, and sends new job alerts to Discord.

## What it does

* Fetches jobs from supported sources:
  * Greenhouse
  * Lever
  * Ashby
  * Workday
  * Phenom embedded pages
  * Entertime
  * Custom HTML sources

* Filters roles by:
  * title keywords
  * locations
  * excluded keywords
* Tracks previously seen jobs in `state_seen.json`
* Sends alerts for only new matching roles to a Discord webhook

## Files

* `watcher.py` - main bot logic
* `config.json` - sources and filters
* `state_seen.json` - remembered job IDs already seen

## Output

The bot prints:

* jobs fetched per source
* total fetched jobs
* matching jobs
* new jobs
* Discord delivery status

## License

Use and modify freely for personal job tracking.
