# Changelog

## 1.1.0
 * Updates for python 3.11 [#26](https://github.com/singer-io/tap-tiktok-ads/pull/26)

## 1.0.0
 * Tiktok API version upgrade from v1.2 to v1.3 [#24](https://github.com/singer-io/tap-tiktok-ads/pull/24)
    * Contains the schema changes as per the new API.
    * Boolean Types were represented as 0/1 previously which will be changed to true/false.
    * Removal of query parameters, changes in URL and response Structure.
    * Authentication Endpoints upgrade.
    * Removal and renaming of fields, update data type of fields.
  * Introduced new feature to fetch the deleted records [#23](https://github.com/singer-io/tap-tiktok-ads/pull/23)

## 0.4.0
 * Sync records from the start date specified in the config. [#22](https://github.com/singer-io/tap-tiktok-ads/pull/22)

## 0.3.0
 * Add backoff logic for error codes - 40200, 40201, 40202, 40700, 50002 [#21](https://github.com/singer-io/tap-tiktok-ads/pull/21)

## 0.2.1
 * Removed Missing Schema items from `adgroups` and `campaign` stream [#15](https://github.com/singer-io/tap-tiktok-ads/pull/15)
 * Fixed Bookmarking logic
 * Fixed Compatibility with sandbox accounts


## 0.2.0
  * Implement request timeout [#3](https://github.com/singer-io/tap-tiktok-ads/pull/3)
  * Dict based to class based refactoring [#4](https://github.com/singer-io/tap-tiktok-ads/pull/4)
  * Updated bookmarking strategy [#5](https://github.com/singer-io/tap-tiktok-ads/pull/5)
  * Add custom error handling and backoff [#6](https://github.com/singer-io/tap-tiktok-ads/pull/6)
  * Add sandbox url [#7](https://github.com/singer-io/tap-tiktok-ads/pull/7)
  * Make field changes required due to API upgrade [#8](https://github.com/singer-io/tap-tiktok-ads/pull/8)
  * Add missing tap-tester tests [#9](https://github.com/singer-io/tap-tiktok-ads/pull/9)
  * Check account access [#10](https://github.com/singer-io/tap-tiktok-ads/pull/10)
  * Update the value types of the config params [#12](https://github.com/singer-io/tap-tiktok-ads/pull/12)
