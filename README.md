# tap_tiktok_ads

This is a [Singer](https://singer.io) tap that produces JSON-formatted
data from the TikTok Marketing API following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap: 

- Pulls raw data from the [TikTok Marketing API](https://ads.tiktok.com/marketing_api/docs). 
- Extracts the following resources from TikTok Marketing API for a single repository:
	- [Accounts](https://ads.tiktok.com/marketing_api/docs?id=1739593083610113) - Endpoint: https://business-api.tiktok.com/open_api/v1.3/advertiser/info/ 
	- [Campaigns](https://ads.tiktok.com/marketing_api/docs?id=1739315828649986) - Endpoint: https://ads.tiktok.com/open_api/v1.3/campaign/get/ 
	- [Adgroups](https://ads.tiktok.com/marketing_api/docs?id=1739314558673922) - Endpoint: https://ads.tiktok.com/open_api/v1.3/adgroup/get/ 
	- [Ads](https://ads.tiktok.com/marketing_api/docs?id=1735735588640770) - Endpoint: https://business-api.tiktok.com/open_api/v1.3/ad/get/ 
	- [Reporting](https://ads.tiktok.com/marketing_api/docs?id=1751087777884161) - Endpoint: https://ads.tiktok.com/open_api/v1.3/report/integrated/get/ 
		- [Ad Insights](https://ads.tiktok.com/marketing_api/docs?id=1738864915188737)
		- [Ad Insights by Age and Gender](https://ads.tiktok.com/marketing_api/docs?id=1738864928947201)
		- [Ad Insights by Country](https://ads.tiktok.com/marketing_api/docs?id=1738864928947201)
		- [Ad Insights by Platform](https://ads.tiktok.com/marketing_api/docs?id=1738864928947201)
        - [Campaign Insights by Province](https://ads.tiktok.com/marketing_api/docs?id=1738864928947201)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Authentication

The tap uses an access token provided by the TikTok Marketing API to make the calls to the API. The access token does not expire, but it'll become invalid if the advertiser cancels the authorization. An invalid access token cannot be renewed or refreshed. THis access token can be obtained by following the step-by-step instructions on the [TikTok Marketing API Docs - Authorization](https://ads.tiktok.com/marketing_api/docs?id=1701890912382977) and [TikTok Marketing API Docs - Authentication](https://ads.tiktok.com/marketing_api/docs?id=1701890914536450).

## Config File

The tap requires some fields to be completed in a config file in order to work. Currently the tap supports the fields:
- start_date: Date from which the tap starts extracting data. RFC3339 format.
- end_date: Date from which the tap ends extracting data. RFC3339 format. (Optional)
- user_agent: User agent that makes the API call.
- access_token: Access token for the TikTok Marketing API
- accounts: A string containing comma-separated values of account ids.
- request_timeout: The time for which request should wait to get response. It is an optional parameter and default value as 300 seconds.
- sandbox (string, optional): Whether to communication with tiktok-ads's sandbox or business account for this application. If you're not sure leave out. Defaults to false.

```json
{
  "start_date": "2019-01-01T00:00:00Z",
  "end_date": "2020-01-01T00:00:00Z",
  "user_agent": "tap-tiktok-ads <api_user_email@your_company.com>",
  "access_token": "YOUR_ACCESS_TOKEN",
  "accounts": "id1, id2, id3",
  "sandbox": "<true|false>",
  "request_timeout": 300
}
```

## Install

Clone this repository, and then install using setup.py. We recommend using a virtualenv:

```bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > python setup.py install
    OR
    > cd .../tap-tiktok-ads
    > pip install .
```

## Dependent libraries

The following dependent libraries were installed.
```bash
    > pip install singer-python
    > pip install singer-tools
    > pip install target-stitch
    > pip install target-json
```

## Setup

Create your tap's `config.json` file which should look like the following:

    ```json
    {
        "start_date": "2019-01-01T00:00:00Z",
        "user_agent": "tap-tiktok-ads <api_user_email@your_company.com>",
        "access_token": "YOUR_ACCESS_TOKEN",
        "accounts": "id1, id2, id3",
        "request_timeout": 300
    }
    ```
    
    Optionally, also create a `state.json` file. `currently_syncing` is an optional attribute used for identifying the last object to be synced in case the job is interrupted mid-stream. The next run would begin where the last job left off.

    ```json
    {
        "currently_syncing": "ad_insights",
        "bookmarks": {
            "accounts": "2020-04-06 10:52:54.000000Z",
            "campaigns": "2021-08-10T06:56:51.000000Z",
            "adgroups": "2021-08-10T06:56:51.000000Z",
            "ads": "2021-08-09T09:11:54.000000Z",
            "ad_insights": "2021-08-18T00:00:00.000000Z",
            "ad_insights_by_age_and_gender": "2021-08-18T00:00:00.000000Z",
            "ad_insights_by_country": "2021-08-18T00:00:00.000000Z",
            "ad_insights_by_platform": "2021-08-18T00:00:00.000000Z"
        }
    }
    ```

## Discovery Mode

Run the Tap in Discovery Mode. This creates a catalog.json for selecting objects/fields to integrate:
    
```bash
> tap-tiktok-ads --config config.json --discover > catalog.json
```
   
See the Singer docs on discovery mode [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

## Sync Mode

Run the Tap in Sync Mode (with catalog) and [write out to state file](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-a-singer-tap-with-a-singer-target)

For Sync mode:
```bash
> tap-tiktok-ads --config tap_config.json --catalog catalog.json > state.json
> tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
```
To load to json files to verify outputs:
```bash
> tap-tiktok-ads --config tap_config.json --catalog catalog.json | target-json > state.json
> tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
```
To pseudo-load to [Stitch Import API](https://github.com/singer-io/target-stitch) with dry run:
```bash
> tap-tiktok-ads --config tap_config.json --catalog catalog.json | target-stitch --config target_config.json --dry-run > state.json
> tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
```

## Test the Tap

While developing the Linkedin Ads tap, the following utilities were run in accordance with Singer.io best practices:
Pylint to improve [code quality](https://github.com/singer-io/getting-started/blob/master/docs/BEST_PRACTICES.md#code-quality)
```bash
> pylint tap_<tap_name> --disable 'broad-except,chained-comparison,empty-docstring,fixme,invalid-name,line-too-long,missing-class-docstring,missing-function-docstring,missing-module-docstring,no-else-raise,no-else-return,too-few-public-methods,too-many-argument
```
```bash
Your code has been rated at 10.00/10 (previous run: 10.00/10, +0.00)
```

To [check the tap](https://github.com/singer-io/singer-tools#singer-check-tap) and verify working:
```bash
> tap-tiktok-ads --config tap_config.json --catalog catalog.json | singer-check-tap > state.json
> tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
```
Check tap resulted in the following:
```bash
The output is valid.
It contained 12135 messages for 8 streams.

      8 schema messages
  11593 record messages
    534 state messages

Details by stream:
+-------------------------------+---------+---------+
| stream                        | records | schemas |
+-------------------------------+---------+---------+
| advertisers                   | 1       | 1       |
| campaigns                     | 23      | 1       |
| adgroups                      | 29      | 1       |
| ads                           | 67      | 1       |
| ad_insights                   | 955     | 1       |
| ad_insights_by_age_and_gender | 6908    | 1       |
| ad_insights_by_country        | 1415    | 1       |
| ad_insights_by_platform       | 2195    | 1       |
+-------------------------------+---------+---------+
```
---

Copyright &copy; 2021 Stitch
