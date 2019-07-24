# Logstash Input for CloudWatch Logs

[![Gem][ico-version]][link-rubygems]
[![Downloads][ico-downloads]][link-rubygems]
[![Software License][ico-license]](LICENSE.md)
[![Build Status][ico-travis]][link-travis]

> Stream events from CloudWatch Logs.

### Purpose
Specify an individual log group or array of groups, and this plugin will scan
all log streams in that group, and pull in any new log events.

Optionally, you may set the `log_group_prefix` parameter to true
which will scan for all log groups matching the specified prefix(s)
and ingest all logs available in all of the matching groups.

## Usage

### Parameters
| Parameter | Input Type | Required | Default |
|-----------|------------|----------|---------|
| log_group | string or Array of strings | Yes | |
| log_group_prefix | boolean | No | `false` |
| start_position | `beginning`, `end`, or an Integer | No | `beginning` |
| lookback_duration | number | No | nil
| sincedb_path | string | No | `$HOME/.sincedb*` |
| interval | number | No | 60 |
| aws_credentials_file | string | No | |
| access_key_id | string | No | |
| secret_access_key | string | No | |
| session_token | string | No | |
| region | string | No | `us-east-1` |
| codec | string | No | `plain` |

#### `start_position`
The `start_position` setting allows you to specify where to begin processing
a newly encountered log group on plugin boot. Whether the group is 'new' is
determined by whether or not the log group has a previously existing entry in
the sincedb file.

Valid options for `start_position` are:
* `beginning` - Reads from the beginning of the group (default)
* `end` - Sets the sincedb to now, and reads any new messages going forward
* Integer - Number of seconds in the past to begin reading at

#### `lookback_duration`
If set, the `lookback_duration` setting allows you to specify a window of time
you want the plugin to revisit during each processing loop. The purpose of this
is to ensure that no CloudWatch events are missed due to the "eventually
consistent read" nature of CloudWatch. Without this setting, the plugin always
calls the CloudWatch "filter_log_events" API with a "start_time" value equal
to the CloudWatch timestamp of the last event it processed at the end of the
previous "filter_log_events" call, plus 1 millisecond. In that case, the call
would not pick up any CloudWatch events whose timestamp is within the previously
visited time window, but had not yet become visible, due to the read consistency
issue. When using this setting, the plugin will subtract its value from the
time at which it started processing for the current interval and store that
value in the sincedb file as the next "start_time" to be used for the next
interval.

Note that, while setting this to a value just large enough to account for
CloudWatch's consistency delays (for most cases, 60 should be more than
sufficient) will protect against missed events, you will need to apply a
strategy within your Logstash inputs pipeline to de-duplicate events, since the
plugin will process some events more than once. This is a necessary trade-off
between guaranteed delivery of all events and dealing with duplicate events and
additional consumer-side volume. Using a small value for this setting may turn
out to be a reasonable compromise in such cases.

Valid options for `lookback_duration` are:
* Integer - Number of seconds in the past to revisit during each
"filter_log_events" API call

#### Logstash Default config params
Other standard logstash parameters are available such as:
* `add_field`
* `type`
* `tags`

### Example

    input {
        cloudwatch_logs {
            log_group => [ "/aws/lambda/my-lambda" ]
            access_key_id => "AKIAXXXXXX"
            secret_access_key => "SECRET"
        }
    }

## Development
The [default logstash README](DEVELOPER.md) which contains development directions and other information has been moved to [DEVELOPER.md](DEVELOPER.md).

## Contributing

All contributions are welcome: ideas, patches, documentation, bug reports, complaints, and even something you drew up on a napkin.

Programming is not a required skill. Whatever you've seen about open source and maintainers or community members  saying "send patches or die" - you will not see that here.

It is more important to the community that you are able to contribute.

For more information about contributing, see the [CONTRIBUTING](https://github.com/elasticsearch/logstash/blob/master/CONTRIBUTING.md) file.

[ico-version]: https://img.shields.io/gem/v/logstash-input-cloudwatch_logs.svg?style=flat-square
[ico-downloads]: https://img.shields.io/gem/dt/logstash-input-cloudwatch_logs.svg?style=flat-square
[ico-license]: https://img.shields.io/badge/License-Apache%202.0-blue.svg?style=flat-square
[ico-travis]: https://img.shields.io/travis/lukewaite/logstash-input-cloudwatch-logs.svg?style=flat-square

[link-rubygems]: https://rubygems.org/gems/logstash-input-cloudwatch_logs
[link-travis]: https://travis-ci.org/lukewaite/logstash-input-cloudwatch_logs
